import { useEffect, useRef, useState } from "react";
import { useConvex, useQuery } from "convex/react";
import { Skeleton, Thumbnail, Token, useLightbox } from "@astryxdesign/core";
import { api } from "@/convex/_generated/api";
import type { Id } from "../lib/model";
import { downloadBytes, formatBytes, isAudioType, isImageType } from "../lib/attachments";
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

/** One message's attachment URL (file storage), queried only once `isEnabled` turns true. */
function useAttachmentUrl(messageId: Id<"messages">, isEnabled: boolean): string | undefined {
  return useQuery(api.messages.attachmentUrl, isEnabled ? { messageId } : "skip") ?? undefined;
}

export function Attachment({ message }: { message: MessageSummary }) {
  if (isImageType(message.attachmentType)) return <ImageAttachment message={message} />;
  if (isAudioType(message.attachmentType)) return <AudioAttachment message={message} />;
  return <FileAttachment message={message} />;
}

function ImageAttachment({ message }: { message: MessageSummary }) {
  const [ref, isNear] = useNearViewport<HTMLDivElement>();
  const src = useAttachmentUrl(message._id, isNear);
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
  const src = useAttachmentUrl(message._id, isNear);
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
  const convex = useConvex();
  const [isLoading, setLoading] = useState(false);
  const name = message.attachmentName ?? "Attachment";
  async function download() {
    setLoading(true);
    try {
      const url = await convex.query(api.messages.attachmentUrl, { messageId: message._id });
      if (url) {
        const bytes = new Uint8Array(await (await fetch(url)).arrayBuffer());
        downloadBytes(bytes, name, message.attachmentType);
      }
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
