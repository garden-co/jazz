import { schema as s } from "jazz-tools";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants.js";

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

const isAdmin = eq(session.claims.role, "admin");
const isMemberOrAdmin = isIn(session.claims.role, ["admin", "member"]);

const inAnnouncements = { chat_id: ANNOUNCEMENTS_CHAT_ID };
const inGeneralChat = { chat_id: CHAT_ID };

const createdByMe = { "$createdBy.account": session.user };

// Members may change their own generic-chat messages; admins may change any.
const canMutateGenericChat = anyOf(createdByMe, isAdmin);

export const permissions = definePermissions({
  messages: {
    read: anyOf(inAnnouncements, allOf(inGeneralChat, isMemberOrAdmin)),
    insert: anyOf(allOf(inAnnouncements, isAdmin), allOf(inGeneralChat, isMemberOrAdmin)),
    updateIf: anyOf(allOf(inAnnouncements, isAdmin), allOf(inGeneralChat, canMutateGenericChat)),
    // Keep the old room's role/ownership decision paired with the same room
    // after the update: this rejects moving a message to another chat, even
    // when an admin is otherwise allowed in both.
    updateTo: (message, old) => ({ chat_id: old.chat_id }),
    delete: anyOf(allOf(inAnnouncements, isAdmin), allOf(inGeneralChat, canMutateGenericChat)),
  },
});
