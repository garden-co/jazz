import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table(
    {
      name: s.string(),
    },
    { todosViaProject: s.reverse("todos", "project") },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      description: s.string().optional(),
      parentId: s.uuid().optional(),
      projectId: s.uuid().optional(),
      owner_id: s.uuid(),
    },
    {
      parent: s.rel("todos", "parentId"),
      todosViaParent: s.reverse("todos", "parent"),
      project: s.rel("projects", "projectId"),
    },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
