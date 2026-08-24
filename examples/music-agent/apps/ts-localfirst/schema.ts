import { schema as s } from "jazz-tools";

const schema = {
  conversations: s.table({
    title: s.string(),
    created_at: s.timestamp(),
  }),
  turns: s.table({
    conversation_id: s.ref("conversations"),
    role: s.enum("user", "assistant", "tool"),
    ordinal: s.int(),
    // Streamed assistant prose is still an ordinary logical Text column.
    body: s.string(),
    created_at: s.timestamp(),
  }),
  tool_calls: s.table({
    turn_id: s.ref("turns"),
    name: s.string(),
    arguments_json: s.string(),
    result_json: s.string(),
  }),
  attachments: s.table({
    turn_id: s.ref("turns"),
    filename: s.string(),
    media_type: s.string(),
    payload: s.bytes(),
    byte_length: s.int(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Conversation = s.RowOf<typeof app.conversations>;
export type Turn = s.RowOf<typeof app.turns>;
