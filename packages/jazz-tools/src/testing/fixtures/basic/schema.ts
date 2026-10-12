import { schema as s } from "../../../schema-namespace.js";
import { defineApp, type Schema, type App } from "../../../typed-app.js";

const schema = {
  todos: {
    title: s.string(),
    done: s.boolean(),
  },
};

type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);
