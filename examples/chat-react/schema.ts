import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
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
});

const { definePermissions, exists, allowedTo, session, anyOf, allOf, ne } = schema;

const isChatMember = (chatId: s.Uuid) => exists.chatMembers({ chatId, userId: session.user });

const mine = { userId: session.user };

const createdByMe = { $createdBy: session.user };

export const permissions = definePermissions({
  canvases: {
    read: (canvas) => allowedTo.read(canvas.chat),
    insert: (canvas) => isChatMember(canvas.chatId),
  },
  chatMembers: {
    read: (chatMember) => anyOf(mine, isChatMember(chatMember.chatId)),
    insert: (chatMember) =>
      anyOf(
        allOf(mine, chatMember.chat.where({ isPublic: true })),
        allOf(mine, chatMember.chat.where(createdByMe)),
        allOf(
          mine,
          { joinCode: ne(null) },
          chatMember.chat.where({ joinCode: chatMember.joinCode }),
        ),
      ),
    delete: mine,
  },
  chats: {
    read: (chat) =>
      anyOf({ isPublic: true }, isChatMember(chat.id), { joinCode: session.claims.join_code }),
    insert: createdByMe,
    updateIf: (chat) => isChatMember(chat.id),
    updateTo: (chat, old) => allOf(isChatMember(chat.id), { isPublic: old.isPublic }),
  },
  messages: {
    read: (message) => anyOf(message.chat.where({ isPublic: true }), isChatMember(message.chatId)),
    insert: (message) => allOf(isChatMember(message.chatId), message.sender.where(mine)),
    delete: (message) => message.sender.where(mine),
  },
  profiles: {
    read: true,
    insert: mine,
    update: mine,
  },
  reactions: {
    read: (reaction) => allowedTo.read(reaction.message),
    insert: mine,
    delete: mine,
  },
  strokes: {
    read: (stroke) => allowedTo.read(stroke.canvas),
    insert: (stroke) => allowedTo.insert(stroke.canvas),
    delete: createdByMe,
  },
});
