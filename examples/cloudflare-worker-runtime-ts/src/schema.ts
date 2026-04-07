import { col } from "../../../packages/jazz-tools/src/dsl.js";
import {
  defineApp,
  defineTable,
  type App,
  type RowOf,
  type Schema,
} from "../../../packages/jazz-tools/src/typed-app.js";

const schema = {
  todos: defineTable({
    title: col.string(),
    done: col.boolean(),
  }),
};

type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);
export type Todo = RowOf<typeof app.todos>;
