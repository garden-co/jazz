import { useEffect, useState } from "react";
import { ChatMetadata } from "./ChatMetadata.js";
import { Item, ItemContent } from "../ui/item.js";
import { IMAGE_TYPES, MAX_IMAGE_BYTES } from "../../chat.js";
import type { Message } from "../../../schema.js";

// Adapted from chat-react's ChatMessage: same alignment, Item bubble and metadata.
export function ChatMessage({ message, isMe }: { message: Message; isMe: boolean }) {
  const [image, setImage] = useState<{ payload: Uint8Array; mimeType: string; url: string }>();
  useEffect(() => {
    const { payload, mimeType } = message;
    if (
      !payload ||
      !mimeType ||
      !IMAGE_TYPES.some((type) => type === mimeType) ||
      payload.byteLength > MAX_IMAGE_BYTES
    )
      return;
    // Query data has already passed the SDK's authenticated encrypted-cell decoder.
    // Blob needs an ArrayBuffer-backed view even when SDK typings allow SharedArrayBuffer.
    const url = URL.createObjectURL(new Blob([new Uint8Array(payload)], { type: mimeType }));
    setImage({ payload, mimeType, url });
    return () => URL.revokeObjectURL(url);
  }, [message.payload, message.mimeType]);
  const url =
    image?.payload === message.payload && image?.mimeType === message.mimeType
      ? image.url
      : undefined;
  return (
    <article
      className={`max-w-7/8 flex flex-col ${isMe ? "self-end items-end" : "self-start items-start"}`}
    >
      <ChatMetadata
        date={message.$createdAt}
        senderName={isMe ? "You" : message.senderId.slice(0, 8)}
      />
      <Item
        className={`max-w-full inline-flex px-2 pt-0 py-1 shadow-xs ${isMe ? "border-0 bg-primary-500 text-white" : "bg-background"}`}
      >
        <ItemContent className="mt-0 text-base relative">
          {message.text && <p className="whitespace-pre-wrap wrap-anywhere">{message.text}</p>}
          {message.payload &&
            (url ? (
              <div className="py-2 flex flex-col gap-2">
                <img
                  src={url}
                  alt={message.filename ?? "Encrypted image"}
                  className="max-h-80 max-w-full rounded-sm object-contain"
                />
                <a href={url} download={message.filename ?? "image"} className="underline text-sm">
                  Download image
                </a>
              </div>
            ) : (
              <p className="text-sm">Image unavailable or unsupported.</p>
            ))}
        </ItemContent>
      </Item>
    </article>
  );
}
