import { schema as s } from "jazz-tools";
import artifact from "./jazz.app.json" with { type: "json" };

const schema = {
  projects: s.table(
    {
      name: s.string(),
    },
    { todos: s.reverse("todos", "projectRelation") },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      priority: s.int().optional(),
      description: s.string().optional(),
      parent: s.uuid().optional(),
      project: s.uuid().optional(),
    },
    {
      parentRelation: s.rel("todos", "parent"),
      children: s.reverse("todos", "parentRelation"),
      projectRelation: s.rel("projects", "project"),
    },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema, { artifact });
