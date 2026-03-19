import { col } from "../../../../src/dsl.js";
import { defineApp, type Schema, type RowOf, type App } from "../../../../src/typed-app.js";

const schema = {
  users: {
    name: col.string(),
    friendsIds: col.array(col.ref("users")),
  },
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    done: col.boolean(),
    tags: col.array(col.string()),
    projectId: col.ref("projects"),
    ownerId: col.ref("users").optional(),
    assigneesIds: col.array(col.ref("users")),
  },
};

export type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);

export type User = RowOf<typeof app.users>;
export type Project = RowOf<typeof app.projects>;
export type Todo = RowOf<typeof app.todos>;
