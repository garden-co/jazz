import { schema as s } from "jazz-tools";
import { e2eeAccountRelation } from "jazz-tools/e2ee";

export const app = s.defineApp({
  chats: s.table({ ownerId: s.uuid() }, { owner: e2eeAccountRelation("ownerId") }),
  messages: s
    .table({ chatId: s.uuid(), text: s.string() }, { chat: s.rel("chats", "chatId") })
    .encrypted({ space: "chatId", columns: ["text"] }),
});
