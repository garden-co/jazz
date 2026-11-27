import { useEffect, useRef, useState } from "react";
import { unreadableRows, useDb } from "jazz-tools/react";
import { Banner, Button, Skeleton, Thumbnail, Token, useLightbox } from "@astryxdesign/core";
import { app } from "../../jazz.generated";
import { failureOf } from "../lib/device-state";
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

/** Read one attachment when its preview comes near the viewport. */
function useAttachmentBytes(messageId: string, isEnabled: boolean) {
  const db = useDb();
  const [result, setResult] = useState<{ bytes?: Uint8Array; reason?: string }>({});
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    if (!isEnabled) return;
    const onError = (error: unknown) => setResult({ reason: failureOf(error).message });
    try {
      return db.subscribe(app.messages.where({ id: messageId }).select("roomId", "attachment"), {
        onUpdate(rows) {
          const bytes = rows[0]?.attachment;
          setResult(
            bytes
              ? { bytes }
              : { reason: unreadableRows(rows)[0]?.code ?? "The attachment data is missing." },
          );
        },
        onError,
      });
    } catch (error) {
      onError(error);
    }
  }, [attempt, db, isEnabled, messageId]);

  return { ...result, retry: () => setAttempt((current) => current + 1) };
}

function LockedAttachment({
  reason,
  retry,
  retryLabel = "Retry preview",
  isLoading,
}: {
  reason: string;
  retry: () => void;
  retryLabel?: string;
  isLoading?: boolean;
}) {
  return (
    <Banner
      status="warning"
      title="Attachment is unreadable on this browser"
      description={reason}
      container="section"
      collapsible={false}
      endContent={<Button label={retryLabel} size="sm" onClick={retry} isLoading={isLoading} />}
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
  const { bytes, reason, retry } = useAttachmentBytes(message.id, isNear);
  const src = useObjectUrl(bytes, message.attachmentType);
  const name = message.attachmentName ?? "Image";
  const lightbox = useLightbox({ media: { src: src ?? "", alt: name, caption: name } });
  if (reason !== undefined) return <LockedAttachment reason={reason} retry={retry} />;
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
  const { bytes, reason, retry } = useAttachmentBytes(message.id, isNear);
  const src = useObjectUrl(bytes, message.attachmentType);
  if (reason !== undefined) return <LockedAttachment reason={reason} retry={retry} />;
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
      if (!bytes) {
        setReason(unreadableRows(rows)[0]?.code ?? "The attachment data is missing.");
        return;
      }
      downloadBytes(bytes, name, message.attachmentType);
      setReason(undefined);
    } catch (error) {
      setReason(failureOf(error).message);
    } finally {
      setLoading(false);
    }
  }
  if (reason !== undefined) {
    return (
      <LockedAttachment
        reason={reason}
        retry={() => void download()}
        retryLabel="Retry download"
        isLoading={isLoading}
      />
    );
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
