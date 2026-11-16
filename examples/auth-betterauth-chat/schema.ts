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

const { definePermissions, session, anyOf, allOf, eq } = schema;

const roleIsAdmin = eq(session.claims.role, "admin");

export const permissions = definePermissions({
  ...betterauthPermissions,
  messages: {
    read: anyOf({ chat_id: "announcements" }, { chat_id: "chat-01" }),
    insert: anyOf(allOf({ chat_id: "announcements" }, roleIsAdmin), { chat_id: "chat-01" }),
    update: anyOf(allOf({ chat_id: "announcements" }, roleIsAdmin), {
      chat_id: "chat-01",
      $createdBy: session.user,
    }),
    delete: anyOf(allOf({ chat_id: "announcements" }, roleIsAdmin), {
      chat_id: "chat-01",
      $createdBy: session.user,
    }),
  },
});
