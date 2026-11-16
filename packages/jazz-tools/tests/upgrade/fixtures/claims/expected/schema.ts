import { schema as s } from "jazz-tools";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants";

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

const rule1 = allOf({ chat_id: CHAT_ID }, isIn(session.claims.role, ["admin", "member"]));

const roleIsAdmin = eq(session.claims.role, "admin");

const rule2 = allOf({ chat_id: ANNOUNCEMENTS_CHAT_ID }, roleIsAdmin);

const rule3 = anyOf(
  rule2,
  allOf({ chat_id: CHAT_ID }, anyOf({ "$createdBy.account": session.user }, roleIsAdmin)),
);

export const permissions = definePermissions({
  messages: {
    read: anyOf({ chat_id: ANNOUNCEMENTS_CHAT_ID }, rule1),
    insert: anyOf(rule2, rule1),
    update: rule3,
    delete: rule3,
  },
});
