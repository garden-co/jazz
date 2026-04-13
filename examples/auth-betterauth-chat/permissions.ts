import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

const ANNOUNCEMENTS_CHAT_ID = process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID!;
const CHAT_ID = process.env.NEXT_PUBLIC_CHAT_ID!;

export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const isMemberOrAdmin = session.where({ "claims.role": { in: ["admin", "member"] } });
  const canMutateGenericChat = anyOf([{ $createdBy: session.user_id }, isAdmin]);

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));

  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]))
    .whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowUpdate
    .whereOld(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]))
    .whereNew({ chat_id: CHAT_ID });

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});
