import { schema as s } from "jazz-tools";
import artifact from "./jazz.app.json" with { type: "json" };

const schema = {
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
    },
    {},
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema, { artifact });
