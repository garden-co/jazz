import { schema as s } from "jazz-tools";

// #region schema
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
const { definePermissions } = schema;

export const permissions = definePermissions({
  chatInvites: {},
  chatMembers: {},
  chats: {},
});
// #endregion permissions
