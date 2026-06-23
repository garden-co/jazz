import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  // A branch is a normal app row. Its id is the branch id passed to
  // db.branch(...) and useAll(..., { branch }).
  branches: s.table({
    name: s.string(),
    owner_id: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    owner_id: s.string(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Todo = s.RowOf<typeof app.todos>;
export type Branch = s.RowOf<typeof app.branches>;
