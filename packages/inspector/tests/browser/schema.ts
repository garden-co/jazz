import { col, defineApp, definePermissions, type Schema, type App } from "jazz-tools";

const schema = {
  todos: {
    title: col.string(),
    done: col.boolean(),
  },
};

type AppSchema = Schema<typeof schema>;

export const app: App<AppSchema> = defineApp(schema);

// Embedded Inspector hosts are ordinary sessions and must be able to read.
export const permissions = definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.never();
  policy.todos.allowUpdate.where({});
  policy.todos.allowDelete.never();
});

export const standalonePermissions = definePermissions(app, ({ policy }) => {
  // Standalone Inspector admin reads must use SYSTEM authority rather than
  // succeeding only because the application happens to allow public reads.
  policy.todos.allowRead.never();
  policy.todos.allowInsert.never();
  policy.todos.allowUpdate.where({});
  policy.todos.allowDelete.never();
});
