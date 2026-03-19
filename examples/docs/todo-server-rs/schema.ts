import { col, defineApp, type Schema, type App } from "jazz-tools";

const schema = {
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    done: col.boolean(),
    description: col.string().optional(),
    parent: col.ref("todos").optional(),
    project: col.ref("projects").optional(),
  },
};

type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);
