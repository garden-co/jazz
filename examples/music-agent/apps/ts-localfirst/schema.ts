import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  conversations: s.table(
    {
      title: s.string(),
      created_at: s.timestamp(),
    },
    { turnsViaConversation: s.reverse("turns", "conversation") },
  ),
  turns: s.table(
    {
      conversation_id: s.uuid(),
      role: s.enum("user", "assistant", "tool"),
      ordinal: s.int(),
      // Streamed assistant prose is still an ordinary logical Text column.
      body: s.string(),
      created_at: s.timestamp(),
    },
    {
      conversation: s.rel("conversations", "conversation_id"),
      tool_callsViaTurn: s.reverse("tool_calls", "turn"),
      attachmentsViaTurn: s.reverse("attachments", "turn"),
    },
  ),
  tool_calls: s.table(
    {
      turn_id: s.uuid(),
      name: s.string(),
      arguments_json: s.string(),
      result_json: s.string(),
    },
    { turn: s.rel("turns", "turn_id") },
  ),
  attachments: s.table(
    {
      turn_id: s.uuid(),
      filename: s.string(),
      media_type: s.string(),
      payload: s.bytes(),
      byte_length: s.int(),
    },
    { turn: s.rel("turns", "turn_id") },
  ),
});

const { definePermissions } = schema;

export const permissions = definePermissions({
  attachments: {
    read: true,
    insert: true,
    update: true,
  },
  conversations: {
    read: true,
    insert: true,
    update: true,
  },
  tool_calls: {
    read: true,
    insert: true,
    update: true,
  },
  turns: {
    read: true,
    insert: true,
    update: true,
  },
});
