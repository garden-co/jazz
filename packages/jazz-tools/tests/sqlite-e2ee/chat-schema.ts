import { schema as s } from "../../src/schema-namespace.js";
import { definePermissions } from "../../src/permissions/index.js";

export function chatSchema() {
  const app = s.defineApp({
    chats: s.table({ ownerId: s.uuid() }, {}),
    messages: s
      .table({ chatId: s.uuid(), text: s.string() }, { chat: s.rel("chats", "chatId") })
      .encrypted({ space: "chatId", columns: ["text"] }),
  });
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.chats.allowRead.always();
    policy.chats.allowInsert.where({ ownerId: session.user.account });
    policy.messages.allowRead.always();
    policy.messages.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.__e2ee_spaces.allowRead.always();
    policy.__e2ee_spaces.allowInsert.where({ creatorAccountId: session.user.account });
    policy.__e2ee_space_grants.allowRead.always();
    policy.__e2ee_space_grants.allowInsert.where({ authorAccountId: session.user.account });
    policy.__e2ee_space_deliveries.allowRead.always();
    policy.__e2ee_space_deliveries.allowInsert.where({ senderAccountId: session.user.account });
  });
  return { app, permissions };
}
