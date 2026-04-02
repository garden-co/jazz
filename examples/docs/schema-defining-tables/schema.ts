import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Todo = s.RowOf<typeof app.todos>;
export type TodoQueryBuilder = typeof app.todos;
