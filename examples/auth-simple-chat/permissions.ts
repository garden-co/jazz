import { definePermissions } from "jazz-tools/permissions";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants.js";
import { app } from "./schema.js";

// How about having these as server-only permissions and move the shared permissions in the schema?
// Actually, how about moving permissions in the schema, and making it possible to have server-only schema & permissions
export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const isMemberOrAdmin = session.where({ "claims.role": { in: ["admin", "member"] } });
  const canMutateGenericChat = anyOf([{ "$createdBy.account": session.user.account }, isAdmin]);

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));

  // One symmetric predicate checks both the old and new row. Members may
  // update their own generic-chat messages; announcements require an admin.
  const canMutateMessage = anyOf([
    allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]),
    allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]),
  ]);
  policy.messages.allowUpdate.where(canMutateMessage);

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});
