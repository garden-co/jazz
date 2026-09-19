import { schema as s } from "jazz-tools";

const schema = {
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
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Conversation = s.RowOf<typeof app.conversations>;
export type Turn = s.RowOf<typeof app.turns>;
