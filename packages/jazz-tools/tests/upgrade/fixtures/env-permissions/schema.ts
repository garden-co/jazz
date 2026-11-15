import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

const schema = {
  messages: s.table({ room: s.string(), text: s.string() }, {}),
};

export const app: s.App<s.Schema<typeof schema>> = s.defineApp(schema, { migrations });
