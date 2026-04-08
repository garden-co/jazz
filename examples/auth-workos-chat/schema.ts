import { schema as s } from "jazz-tools";

const schema = {
  messages: s.table({
    author_name: s.string(),
    chat_id: s.string(),
    text: s.string(),
    sent_at: s.timestamp(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Message = s.RowOf<typeof app.messages>;
