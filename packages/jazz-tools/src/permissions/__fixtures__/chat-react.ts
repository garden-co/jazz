// examples/chat-react's schema and permissions in the TS DSL (acceptance fixture).
import { schema as s } from "../../index.js";

export const schema = s.defineSchema({
  profiles: s.table(
    { userId: s.uuid(), name: s.string(), avatar: s.string().optional() },
    { messagesViaSender: s.reverse("messages", "sender") },
  ),
  chats: s.table(
    { name: s.string().optional(), isPublic: s.boolean(), joinCode: s.string().optional() },
    {
      chatMembersViaChat: s.reverse("chatMembers", "chat"),
      messagesViaChat: s.reverse("messages", "chat"),
      canvasesViaChat: s.reverse("canvases", "chat"),
    },
  ),
  chatMembers: s.table(
    { chatId: s.uuid(), userId: s.uuid(), joinCode: s.string().optional() },
    { chat: s.rel("chats", "chatId") },
  ),
  messages: s.table(
    { chatId: s.uuid(), text: s.string(), senderId: s.uuid() },
    {
      chat: s.rel("chats", "chatId"),
      sender: s.rel("profiles", "senderId"),
      reactionsViaMessage: s.reverse("reactions", "message"),
    },
  ),
  reactions: s.table(
    { messageId: s.uuid(), userId: s.uuid(), emoji: s.string() },
    { message: s.rel("messages", "messageId") },
  ),
  canvases: s.table(
    { chatId: s.uuid() },
    { chat: s.rel("chats", "chatId"), strokesViaCanvas: s.reverse("strokes", "canvas") },
  ),
  strokes: s.table(
    { canvasId: s.uuid(), color: s.string(), width: s.int(), pointsJson: s.string() },
    { canvas: s.rel("canvases", "canvasId") },
  ),
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf, ne } = schema;

const mine = { userId: session.user };
const createdByMe = { $createdBy: session.user };
const isMember = (chatId: s.Uuid) => exists.chatMembers({ chatId, userId: session.user });

export const permissions = definePermissions({
  profiles: { read: true, insert: mine, update: mine },
  chats: {
    read: (chat) =>
      anyOf({ isPublic: true }, isMember(chat.id), { joinCode: session.claims.join_code }),
    insert: createdByMe,
    updateIf: (chat) => isMember(chat.id),
    // Members may rename a chat, but not make it public or private.
    updateTo: (chat, old) => allOf(isMember(chat.id), { isPublic: old.isPublic }),
  },
  chatMembers: {
    read: (member) => anyOf(mine, isMember(member.chatId)),
    insert: (member) =>
      allOf(
        mine,
        anyOf(
          member.chat.where({ isPublic: true }),
          member.chat.where(createdByMe),
          allOf({ joinCode: ne(null) }, member.chat.where({ joinCode: member.joinCode })),
        ),
      ),
    delete: mine,
  },
  messages: {
    read: (msg) => anyOf(msg.chat.where({ isPublic: true }), isMember(msg.chatId)),
    insert: (msg) => allOf(isMember(msg.chatId), msg.sender.where(mine)),
    delete: (msg) => msg.sender.where(mine),
  },
  reactions: { read: (r) => allowedTo.read(r.message), insert: mine, delete: mine },
  canvases: { read: (c) => allowedTo.read(c.chat), insert: (c) => isMember(c.chatId) },
  strokes: {
    read: (st) => allowedTo.read(st.canvas),
    insert: (st) => allowedTo.insert(st.canvas),
    delete: createdByMe,
  },
});
