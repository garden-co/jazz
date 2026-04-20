import { definePermissions } from "jazz-tools/permissions";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants";
import { app } from "./schema";

export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAuthenticated = session.where({ authMode: "external" });
  const canMutateGenericChat = { $createdBy: session.user_id };

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where({ chat_id: CHAT_ID });

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAuthenticated]));
  policy.messages.allowInsert.where({ chat_id: CHAT_ID });

  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAuthenticated]))
    .whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]))
    .whereNew({ chat_id: CHAT_ID });

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAuthenticated]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});
