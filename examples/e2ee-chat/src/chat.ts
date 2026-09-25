import type { Db } from "jazz-tools";
import { v5 as uuidv5, validate as isUuid } from "uuid";
import { app } from "../schema.js";

// A stable row identity, not an encryption primitive. Pair encoding is unambiguous.
const MEMBERSHIP_NAMESPACE = "1bed87da-3349-4a4e-a401-84f0ab1b6497";
export const membershipId = (chatId: string, accountId: string): string =>
  uuidv5(JSON.stringify([chatId.toLowerCase(), accountId.toLowerCase()]), MEMBERSHIP_NAMESPACE);

export const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
export const IMAGE_TYPES = ["image/png", "image/jpeg", "image/webp", "image/gif"] as const;

export async function createChat(db: Db, ownerId: string) {
  const tx = db.beginExclusiveTransaction();
  const chat = tx.insert(app.chats, { ownerId });
  tx.insert(app.chatOwners, { chatId: chat.id, accountId: ownerId });
  tx.insert(
    app.chatMembers,
    { chatId: chat.id, accountId: ownerId },
    { id: membershipId(chat.id, ownerId) },
  );
  // The registered scope insert stages its creator's root/grant in this same transaction.
  await tx.commit().wait({ tier: "global" });
  return chat;
}

// #region e2ee-chat-share
export async function shareChat(db: Db, chatId: string, recipientId: string): Promise<void> {
  if (!isUuid(recipientId)) throw new Error("Enter a valid recipient account ID");
  recipientId = recipientId.toLowerCase();
  await db
    .upsert(app.chatMembers, membershipId(chatId, recipientId), {
      chatId,
      accountId: recipientId,
    })
    .wait({ tier: "global" });
  // Separate accepted steps. Failure here does not roll back the membership above.
  // Explicit retry repeats the same row identity and idempotent current-epoch grant.
  await db.e2ee.spaces.grant(app.chats, chatId, recipientId).wait();
}
// #endregion e2ee-chat-share

// #region e2ee-chat-upload
export async function sendMessage(
  db: Db,
  chatId: string,
  senderId: string,
  text: string,
  image: File | null,
  onProgress: (state: "Uploading" | "Awaiting acceptance") => void,
): Promise<void> {
  if (!image && !text.trim()) throw new Error("Write a message or choose an image");
  if (
    image &&
    (!IMAGE_TYPES.some((type) => type === image.type) ||
      image.size > MAX_IMAGE_BYTES ||
      image.size === 0)
  )
    throw new Error("Choose a nonempty PNG, JPEG, WebP or GIF image up to 10 MiB");
  const values = {
    chatId,
    senderId,
    text: text.trim(),
    filename: image?.name ?? null,
    mimeType: image?.type ?? null,
  };
  if (image) {
    onProgress("Uploading");
    const write = await db.insertStreaming(app.messages, { ...values, payload: image.stream() });
    onProgress("Awaiting acceptance");
    await write.wait({ tier: "global" });
  } else {
    onProgress("Awaiting acceptance");
    await db.insert(app.messages, { ...values, payload: null }).wait({ tier: "global" });
  }
}
// #endregion e2ee-chat-upload
