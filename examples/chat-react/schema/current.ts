import { table, col } from "jazz-tools";

table("profiles", {
  userId: col.string(),
  name: col.string(),
  avatar: col.string().optional(),
});

table("chats", {
  name: col.string().optional(),
  isPublic: col.boolean(),
  createdBy: col.string(),
  joinCode: col.string().optional(),
});

table("chatMembers", {
  chatId: col.ref("chats"),
  userId: col.string(),
  joinCode: col.string().optional(),
});

table("messages", {
  chatId: col.ref("chats"),
  text: col.string(),
  senderId: col.ref("profiles"),
  createdAt: col.timestamp(),
});

table("reactions", {
  messageId: col.ref("messages"),
  userId: col.string(),
  emoji: col.string(),
});

table("canvases", {
  chatId: col.ref("chats"),
  createdAt: col.timestamp(),
});

table("strokes", {
  canvasId: col.ref("canvases"),
  ownerId: col.string(),
  color: col.string(),
  width: col.int(),
  pointsJson: col.string(),
  createdAt: col.timestamp(),
});

table("attachments", {
  messageId: col.ref("messages"),
  type: col.string(),
  name: col.string(),
  fileId: col.ref("files"),
  size: col.int(),
});

table("file_parts", {
  data: col.bytes(),
});

table("files", {
  name: col.string().optional(),
  mimeType: col.string(),
  partIds: col.array(col.ref("file_parts")),
  partSizes: col.array(col.int()),
});
