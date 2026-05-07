// permissions.ts
import { definePermissions } from "jazz-tools/permissions";

// constants.ts
var CHAT_ID = "chat-01";
var ANNOUNCEMENTS_CHAT_ID = "announcements";

// schema.ts
import { schema as s } from "jazz-tools";
var schema = {
  messages: s.table({
    author_name: s.string(),
    chat_id: s.string(),
    text: s.string(),
    sent_at: s.timestamp()
  })
};
var app = s.defineApp(schema);

// permissions.ts
var permissions_default = definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  const isAdmin = session.where({ "claims.role": "admin" });
  const isMemberOrAdmin = session.where({ "claims.role": { in: ["admin", "member"] } });
  const canMutateGenericChat = anyOf([{ $createdBy: session.user_id }, isAdmin]);
  policy.messages.allowRead.where({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowRead.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));
  policy.messages.allowInsert.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowInsert.where(allOf([{ chat_id: CHAT_ID }, isMemberOrAdmin]));
  policy.messages.allowUpdate.whereOld(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin])).whereNew({ chat_id: ANNOUNCEMENTS_CHAT_ID });
  policy.messages.allowUpdate.whereOld(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat])).whereNew({ chat_id: CHAT_ID });
  policy.messages.allowDelete.where(allOf([{ chat_id: ANNOUNCEMENTS_CHAT_ID }, isAdmin]));
  policy.messages.allowDelete.where(allOf([{ chat_id: CHAT_ID }, canMutateGenericChat]));
});
export {
  permissions_default as default
};
