import { schema as s } from "jazz-tools";

const schema = {
  todos: s.table({ title: s.string(), done: s.boolean() }, {}),
};
type AppSchema = s.Schema<typeof schema>;
// Memory only, with no server: the app never has another version to migrate.
export const app: s.App<AppSchema> = s.defineApp(schema, { migrations: "none" });
export type Todo = s.RowOf<typeof app.todos>;
