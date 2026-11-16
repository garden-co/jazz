import { schema as s } from "jazz-tools";
import { tables as betterauthSchema } from "./schema-better-auth/schema";
import { permissions as betterauthPermissions } from "./schema-better-auth/schema";

// #region default-stack-schema
export const schema = s.defineSchema({
  ...betterauthSchema,
  messages: s.table(
    {
      author_name: s.string(),
      chat_id: s.string(),
      text: s.string(),
      sent_at: s.timestamp(),
    },
    {},
  ),
});

// #endregion default-stack-schema

// #region default-stack-permissions
const { definePermissions, session, anyOf, allOf, eq } = schema;

const isAdmin = eq(session.claims.role, "admin");

const inAnnouncements = { chat_id: "announcements" };
const inGeneralChat = { chat_id: "chat-01" };

const createdByMe = { "$createdBy.account": session.user };

export const permissions = definePermissions({
  ...betterauthPermissions,
  messages: {
    read: anyOf(inAnnouncements, inGeneralChat),
    insert: anyOf(allOf(inAnnouncements, isAdmin), inGeneralChat),
    // `update` checks the same room/role predicate on both the old and new row,
    // so a General-message creator cannot satisfy the old-row rule and then
    // move the message into Announcements.
    update: anyOf(allOf(inAnnouncements, isAdmin), { ...inGeneralChat, ...createdByMe }),
    delete: anyOf(allOf(inAnnouncements, isAdmin), { ...inGeneralChat, ...createdByMe }),
  },
});
// #endregion default-stack-permissions
