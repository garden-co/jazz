import { col, defineApp, type Schema, type App } from "jazz-tools";

const schema = {
  todos: {
    title: col.string(),
    done: col.boolean(),
  },
};

type AppSchema = Schema<typeof schema>;

export const app: App<AppSchema> = defineApp(schema);
