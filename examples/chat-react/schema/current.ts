import { table, col } from "jazz-tools";

table("profiles", {
  userId: col.string(),
  name: col.string(),
  avatar: col.string().optional(),
});

table("chats", {
  isPublic: col.boolean(),
  createdBy: col.string(),
});

table("chatMembers", {
  chat: col.ref("chats"),
  userId: col.string(),
  joinCode: col.string().optional(),
});

table("messages", {
  chat: col.ref("chats"),
  text: col.string(),
  sender: col.ref("profiles"),
  senderId: col.string(),
  createdAt: col.timestamp(),
});

table("reactions", {
  message: col.ref("messages"),
  userId: col.string(),
  emoji: col.string(),
});

table("canvases", {
  chat: col.ref("chats"),
  createdAt: col.timestamp(),
});

table("strokes", {
  canvas: col.ref("canvases"),
  ownerId: col.string(),
  color: col.string(),
  width: col.int(),
  pointsJson: col.string(),
  createdAt: col.timestamp(),
});

table("attachments", {
  message: col.ref("messages"),
  type: col.string(),
  name: col.string(),
  data: col.string(),
  mimeType: col.string(),
  size: col.int(),
});
