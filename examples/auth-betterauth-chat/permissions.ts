import { definePermissions } from "jazz-tools/permissions";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants";
import { app } from "./schema";

export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const isAuthenticated = session.where({
    "claims.auth_mode": { in: ["local-first", "external"] },
  });
  const canMutateGenericChat = anyOf([{ $createdBy: session.user_id }, isAdmin]);

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where(allOf([{ chat_id: CHAT_ID }, isAuthenticated]));

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where(allOf([{ chat_id: CHAT_ID }, isAuthenticated]));

  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]))
    .whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]))
    .whereNew({ chat_id: CHAT_ID });

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});
