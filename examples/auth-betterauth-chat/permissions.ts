import { definePermissions } from "jazz-tools/permissions";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app } from "./schema";

const ANNOUNCEMENTS_CHAT_ID = process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID!;
const CHAT_ID = process.env.NEXT_PUBLIC_CHAT_ID!;

const messagePermissions = definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const canMutateGenericChat = { $createdBy: session.author };
  // `allowUpdate` is evaluated independently on the old and new row. Use the
  // same room/role predicate for both sides so a General-message creator
  // cannot satisfy the old-row rule then move the message into Announcements.
  const canMutateMessage = anyOf([
    allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]),
    allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]),
  ]);

  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where({ chat_id: CHAT_ID });

  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where({ chat_id: CHAT_ID });

  policy.messages.allowUpdate.where(canMutateMessage);

  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});

export default {
  ...betterAuthPermissions,
  ...messagePermissions,
};
