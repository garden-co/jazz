// #region e2ee-chat-schema
import { schema as s } from "jazz-tools";
import { accountRelation } from "jazz-tools/e2ee";

export const app = s.defineApp({
  chats: s.table({ ownerId: s.uuid() }, {}),
  chatOwners: s.table(
    { chatId: s.uuid(), accountId: s.uuid() },
    { chat: s.rel("chats", "chatId"), account: accountRelation() },
  ),
  chatMembers: s.table(
    { chatId: s.uuid(), accountId: s.uuid() },
    { chat: s.rel("chats", "chatId") },
  ),
  messages: s
    .table(
      {
        chatId: s.uuid(),
        senderId: s.uuid(),
        text: s.string(),
        filename: s.string().optional(),
        mimeType: s.string().optional(),
        payload: s.bytes().optional(),
      },
      { chat: s.rel("chats", "chatId") },
    )
    .encrypted({ space: "chatId", columns: ["text", "filename", "mimeType", "payload"] }),
});

// #endregion e2ee-chat-schema
export type Message = s.RowOf<typeof app.messages> & { $createdAt: Date };
