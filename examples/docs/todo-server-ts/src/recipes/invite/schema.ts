// #region schema
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  chatInvites: s.table(
    { chatId: s.uuid(), code: s.string(), singleUse: s.boolean() },
    {
      chat: s.rel("chats", "chatId"),
    },
  ),
  chatMembers: s.table(
    { chatId: s.uuid(), user_id: s.uuid(), inviteId: s.string().optional() },
    {
      chat: s.rel("chats", "chatId"),
    },
  ),
  chats: s.table(
    {},
    {
      invites: s.reverse("chatInvites", "chat"),
      members: s.reverse("chatMembers", "chat"),
    },
  ),
});
// #endregion schema

// #region permissions
const { definePermissions, exists, session, anyOf, allOf } = schema;

const isChatCreator = (chatId: s.Uuid) => exists.chats({ id: chatId, $createdBy: session.user });

const isChatMember = (chatId: s.Uuid) => exists.chatMembers({ chatId, user_id: session.user });

const mine = { user_id: session.user };

export const permissions = definePermissions({
  chatInvites: {
    read: false,
    insert: (chatInvite) => isChatCreator(chatInvite.chatId),
    delete: (chatInvite) => isChatCreator(chatInvite.chatId),
  },
  chatMembers: {
    read: (chatMember) => anyOf(mine, isChatCreator(chatMember.chatId)),
    insert: (chatMember) => allOf(mine, isChatCreator(chatMember.chatId)),
    delete: (chatMember) => anyOf(mine, isChatCreator(chatMember.chatId)),
  },
  chats: {
    read: (chat) => isChatMember(chat.id),
    insert: true,
  },
});
// #endregion permissions
