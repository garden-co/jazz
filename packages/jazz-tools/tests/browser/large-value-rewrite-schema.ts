import { schema } from "../../src/index.js";

// #3816, shaped like MusicAgent: a conversation holds a large attachment and a
// reply that a backend rewrites word by word. The browser reads both only
// through a conversation it created, and lists attachments without their bytes.
export const largeValueRewriteApp = schema.defineApp({
  conversations: schema.table(
    { title: schema.string() },
    {
      turnsViaConversation: schema.reverse("turns", "conversation"),
      attachmentsViaConversation: schema.reverse("attachments", "conversation"),
    },
  ),
  turns: schema.table(
    { conversation_id: schema.uuid(), body: schema.string() },
    { conversation: schema.rel("conversations", "conversation_id") },
  ),
  attachments: schema.table(
    {
      conversation_id: schema.uuid(),
      filename: schema.string(),
      payload: schema.bytes(),
    },
    { conversation: schema.rel("conversations", "conversation_id") },
  ),
});

export const largeValueRewritePermissions = schema.definePermissions(
  largeValueRewriteApp,
  ({ policy, session, allowedTo }) => [
    policy.conversations.allowRead.where({ $createdBy: session.user }),
    policy.conversations.allowInsert.always(),
    policy.turns.allowRead.where(allowedTo.read("conversation")),
    policy.attachments.allowRead.where(allowedTo.read("conversation")),
  ],
);
