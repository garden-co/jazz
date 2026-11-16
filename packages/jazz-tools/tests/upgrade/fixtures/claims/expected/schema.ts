import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
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

const { definePermissions, session, anyOf, allOf, eq, isIn } = schema;

const roleInAdminOrMember = isIn(session.claims.role, ["admin", "member"]);

const roleIsAdmin = eq(session.claims.role, "admin");

const createdByMe = { $createdBy: session.user };

export const permissions = definePermissions({
  messages: {
    read: anyOf({ chat_id: "announcements" }, allOf({ chat_id: "chat-01" }, roleInAdminOrMember)),
    insert: anyOf(
      allOf({ chat_id: "announcements" }, roleIsAdmin),
      allOf({ chat_id: "chat-01" }, roleInAdminOrMember),
    ),
    update: anyOf(
      allOf({ chat_id: "announcements" }, roleIsAdmin),
      allOf({ chat_id: "chat-01" }, anyOf(createdByMe, roleIsAdmin)),
    ),
    delete: anyOf(
      allOf({ chat_id: "announcements" }, roleIsAdmin),
      allOf({ chat_id: "chat-01" }, anyOf(createdByMe, roleIsAdmin)),
    ),
  },
});
