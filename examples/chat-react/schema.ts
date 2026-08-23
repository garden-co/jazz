import { schema as s } from "jazz-tools";

const schema = {
  profiles: s.table({
    userId: s.string(),
    name: s.string(),
    avatar: s.string().optional(),
  }),
  chats: s.table({
    name: s.string().optional(),
    isPublic: s.boolean(),
    ownerId: s.string(),
    joinCode: s.string().optional(),
  }),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    userId: s.string(),
    joinCode: s.string().optional(),
  }),
  messages: s.table({
    chatId: s.ref("chats"),
    text: s.string(),
    senderId: s.ref("profiles"),
    occurredAt: s.timestamp(),
  }),
  reactions: s.table({
    messageId: s.ref("messages"),
    userId: s.string(),
    emoji: s.string(),
  }),
  canvases: s.table({
    chatId: s.ref("chats"),
    occurredAt: s.timestamp(),
  }),
  strokes: s.table({
    canvasId: s.ref("canvases"),
    ownerId: s.string(),
    color: s.string(),
    width: s.int(),
    pointsJson: s.string(),
    occurredAt: s.timestamp(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Profile = s.RowOf<typeof app.profiles>;
export type Chat = s.RowOf<typeof app.chats>;
export type Message = s.RowOf<typeof app.messages>;
