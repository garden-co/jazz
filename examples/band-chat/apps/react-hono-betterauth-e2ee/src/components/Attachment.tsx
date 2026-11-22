import { useEffect, useRef, useState } from "react";
import { unreadableRows, useAll, useDb } from "jazz-tools/react";
import { Banner, Skeleton, Thumbnail, Token, useLightbox } from "@astryxdesign/core";
import { app } from "../../jazz.generated";
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
function useAttachmentBytes(messageId: string, isEnabled: boolean) {
  const { data, error, unreadable } = useAll(
    isEnabled ? app.messages.where({ id: messageId }).select("roomId", "attachment") : undefined,
  );
  const bytes = data?.[0]?.attachment ?? undefined;
  const reason =
    error?.message ??
    unreadable[0]?.code ??
    (data && !bytes ? "The attachment data is missing." : undefined);
  return { bytes, reason };
}

function LockedAttachment({ reason }: { reason: string }) {
  return (
    <Banner
      status="warning"
      title="Attachment is unreadable on this browser"
      description={reason}
      container="section"
      collapsible={false}
    />
  );
}

export function Attachment({ message }: { message: MessageSummary }) {
  if (isImageType(message.attachmentType)) return <ImageAttachment message={message} />;
  if (isAudioType(message.attachmentType)) return <AudioAttachment message={message} />;
  return <FileAttachment message={message} />;
}

function ImageAttachment({ message }: { message: MessageSummary }) {
  const [ref, isNear] = useNearViewport<HTMLDivElement>();
  const { bytes, reason } = useAttachmentBytes(message.id, isNear);
  const src = useObjectUrl(bytes, message.attachmentType);
  const name = message.attachmentName ?? "Image";
  const lightbox = useLightbox({ media: { src: src ?? "", alt: name, caption: name } });
  if (reason !== undefined) return <LockedAttachment reason={reason} />;
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
  const { bytes, reason } = useAttachmentBytes(message.id, isNear);
  const src = useObjectUrl(bytes, message.attachmentType);
  if (reason !== undefined) return <LockedAttachment reason={reason} />;
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
  const [reason, setReason] = useState<string>();
  const name = message.attachmentName ?? "Attachment";
  async function download() {
    setLoading(true);
    try {
      const rows = await db.all(
        app.messages.where({ id: message.id }).select("roomId", "attachment").limit(1),
      );
      const bytes = rows[0]?.attachment;
      if (bytes) downloadBytes(bytes, name, message.attachmentType);
      else setReason(unreadableRows(rows)[0]?.code ?? "The attachment data is missing.");
    } catch (error) {
      setReason(error instanceof Error ? error.message : String(error));
    } finally {
      setLoading(false);
    }
  }
  if (reason !== undefined) return <LockedAttachment reason={reason} />;
  return (
    <Token
      label={name}
      description={isLoading ? "Downloading…" : `${formatBytes(message.attachmentSize)} · Download`}
      aria-label={`Download ${name}`}
      onClick={() => void download()}
    />
  );
}
