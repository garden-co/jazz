import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    parent: s.ref("todos").optional(),
    project: s.ref("projects").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
