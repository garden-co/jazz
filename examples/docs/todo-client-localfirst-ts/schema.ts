import { schema as s } from "jazz-tools";

const schema = {
  // #region schema-todo-client-ts
  projects: s.table(
    {
      name: s.string(),
    },
    { todos: s.reverse("todos", "project") },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      priority: s.int().optional(),
      description: s.string().optional(),
      owner_id: s.uuid().optional(),
      parentId: s.uuid().optional(),
      projectId: s.uuid().optional(),
    },
    {
      parent: s.rel("todos", "parentId"),
      children: s.reverse("todos", "parent"),
      project: s.rel("projects", "projectId"),
    },
  ),
  // #endregion schema-todo-client-ts
};

// #region schema-define-app-ts
type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Todo = s.RowOf<typeof app.todos>;
// #endregion schema-define-app-ts
