import { definePermissions } from "jazz-tools/permissions";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "./constants";
import { app } from "./schema";

export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where(
    allOf([{ chat_id: CHAT_ID }, session.where({ "claims.role": { in: ["admin", "member"] } })]),
  );

  policy.messages.allowInsert.where(
    allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, session.where({ "claims.role": "admin" })]),
  );
  policy.messages.allowInsert.where(
    allOf([
      { chat_id: CHAT_ID },
      anyOf([{ author_id: session.user_id }, session.where({ "claims.role": "admin" })]),
    ]),
  );

  policy.messages.allowUpdate
    .whereOld(
      allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, session.where({ "claims.role": "admin" })]),
    )
    .whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowUpdate
    .whereOld(
      allOf([
        { chat_id: CHAT_ID },
        anyOf([{ author_id: session.user_id }, session.where({ "claims.role": "admin" })]),
      ]),
    )
    .whereNew({ chat_id: CHAT_ID });

  policy.messages.allowDelete.where(
    allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, session.where({ "claims.role": "admin" })]),
  );
  policy.messages.allowDelete.where(
    allOf([
      { chat_id: CHAT_ID },
      anyOf([{ author_id: session.user_id }, session.where({ "claims.role": "admin" })]),
    ]),
  );
});
