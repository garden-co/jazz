import { schema as s } from "jazz-tools";

// Test tables built with the public DSL (no JSON-like definitions).
const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean().default(false),
    priority: s.string().optional(),
    projectId: s.ref("projects").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;

export const app: s.App<AppSchema> = s.defineApp(schema);

export type Todo = s.RowOf<typeof app.todos>;
export type Project = s.RowOf<typeof app.projects>;
