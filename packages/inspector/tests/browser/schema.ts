import { col, defineApp, definePermissions, type Schema, type App } from "jazz-tools";

const schema = {
  todos: {
    title: col.string(),
    done: col.boolean(),
  },
};

type AppSchema = Schema<typeof schema>;

export const app: App<AppSchema> = defineApp(schema);

export const permissions = definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.never();
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
});
