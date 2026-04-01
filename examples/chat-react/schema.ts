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
    createdBy: s.string(),
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
    createdAt: s.timestamp(),
  }),
  reactions: s.table({
    messageId: s.ref("messages"),
    userId: s.string(),
    emoji: s.string(),
  }),
  canvases: s.table({
    chatId: s.ref("chats"),
    createdAt: s.timestamp(),
  }),
  strokes: s.table({
    canvasId: s.ref("canvases"),
    ownerId: s.string(),
    color: s.string(),
    width: s.int(),
    pointsJson: s.string(),
    createdAt: s.timestamp(),
  }),
  attachments: s.table({
    messageId: s.ref("messages"),
    type: s.string(),
    name: s.string(),
    fileId: s.ref("files"),
    size: s.int(),
  }),
  file_parts: s.table({
    data: s.bytes(),
  }),
  files: s.table({
    name: s.string().optional(),
    mimeType: s.string(),
    partIds: s.array(s.ref("file_parts")),
    partSizes: s.array(s.int()),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Profile = s.RowOf<typeof app.profiles>;
export type Message = s.RowOf<typeof app.messages>;
export type Attachment = s.RowOf<typeof app.attachments>;
