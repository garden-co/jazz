import { definePermissions } from "jazz-tools/permissions";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants.js";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const isMemberOrAdmin = session.where({ "claims.role": { in: ["admin", "member"] } });
  const canMutateGenericChat = anyOf([{ "$createdBy.account": session.user.account }, isAdmin]);

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));

  // Keep the old room's role/ownership decision paired with the same persisted
  // room after the update. `exists` reads before the write, so this rejects a
  // message move even when an admin is otherwise allowed in both chats.
  policy.messages.allowUpdate
    .whereOld(
      anyOf([
        allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]),
        allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]),
      ]),
    )
    .whereNew((message) =>
      policy.messages.exists.where({ id: message.id, chat_id: message.chat_id }),
    );

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});
