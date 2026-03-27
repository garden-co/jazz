import { definePermissions } from "jazz-tools/permissions";
import { ANNOUNCEMENTS_CHAT_ID, CHAT_ID } from "../constants.js";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  // READ
  // announcements: readable by everyone (anonymous included)
  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  // chat-01: readable only by admin or member
  policy.messages.allowRead.where(
    allOf([{ chat_id: CHAT_ID }, session.where({ "claims.role": { in: ["admin", "member"] } })]),
  );

  // INSERT
  // announcements: admin only
  policy.messages.allowInsert.where(
    allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, session.where({ "claims.role": "admin" })]),
  );
  // chat-01: author must be the session user, or session is admin
  policy.messages.allowInsert.where(
    allOf([
      { chat_id: CHAT_ID },
      anyOf([{ author_id: session.user_id }, session.where({ "claims.role": "admin" })]),
    ]),
  );

  // UPDATE
  // announcements: admin only, chat_id must stay "announcements"
  policy.messages.allowUpdate
    .whereOld(
      allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, session.where({ "claims.role": "admin" })]),
    )
    .whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  // chat-01: own row or admin, chat_id must stay "chat-01"
  policy.messages.allowUpdate
    .whereOld(
      allOf([
        { chat_id: CHAT_ID },
        anyOf([{ author_id: session.user_id }, session.where({ "claims.role": "admin" })]),
      ]),
    )
    .whereNew({ chat_id: CHAT_ID });

  // DELETE
  // announcements: admin only
  policy.messages.allowDelete.where(
    allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, session.where({ "claims.role": "admin" })]),
  );
  // chat-01: own row or admin
  policy.messages.allowDelete.where(
    allOf([
      { chat_id: CHAT_ID },
      anyOf([{ author_id: session.user_id }, session.where({ "claims.role": "admin" })]),
    ]),
  );
});
