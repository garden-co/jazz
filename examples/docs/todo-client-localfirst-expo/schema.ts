// #region schema-expo
import { col, defineApp, type Schema, type RowOf, type App } from "jazz-tools";

const schema = {
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    done: col.boolean(),
    description: col.string().optional(),
    ownerId: col.string(),
    parentId: col.ref("todos").optional(),
    projectId: col.ref("projects").optional(),
  },
};

type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);

export type Todo = RowOf<typeof app.todos>;
// #endregion schema-expo
