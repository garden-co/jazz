import { schema as s } from "jazz-tools";

const schema = {
  profiles: s.table(
    {
      userId: s.uuid(),
      name: s.string(),
      avatar: s.string().optional(),
    },
    { messagesViaSender: s.reverse("messages", "sender") },
  ),
  chats: s.table(
    {
      name: s.string().optional(),
      isPublic: s.boolean(),
      joinCode: s.string().optional(),
    },
    {
      chatMembersViaChat: s.reverse("chatMembers", "chat"),
      messagesViaChat: s.reverse("messages", "chat"),
      canvasesViaChat: s.reverse("canvases", "chat"),
    },
  ),
  chatMembers: s.table(
    {
      chatId: s.uuid(),
      userId: s.uuid(),
      joinCode: s.string().optional(),
    },
    { chat: s.rel("chats", "chatId") },
  ),
  messages: s.table(
    {
      chatId: s.uuid(),
      text: s.string(),
      senderId: s.uuid(),
    },
    {
      chat: s.rel("chats", "chatId"),
      sender: s.rel("profiles", "senderId"),
      reactionsViaMessage: s.reverse("reactions", "message"),
    },
  ),
  reactions: s.table(
    {
      messageId: s.uuid(),
      userId: s.uuid(),
      emoji: s.string(),
    },
    { message: s.rel("messages", "messageId") },
  ),
  canvases: s.table(
    {
      chatId: s.uuid(),
    },
    { chat: s.rel("chats", "chatId"), strokesViaCanvas: s.reverse("strokes", "canvas") },
  ),
  strokes: s.table(
    {
      canvasId: s.uuid(),
      color: s.string(),
      width: s.int(),
      pointsJson: s.string(),
    },
    { canvas: s.rel("canvases", "canvasId") },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Profile = s.RowOf<typeof app.profiles>;
export type Chat = s.RowOf<typeof app.chats>;
export type Message = s.RowOf<typeof app.messages> & { $createdAt: Date };
