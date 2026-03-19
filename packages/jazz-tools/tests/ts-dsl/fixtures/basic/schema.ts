import { col } from "../../../../src/dsl.js";
import {
  defineApp,
  type DefinedSchema,
  type RowOf,
  type TypedApp,
} from "../../../../src/typed-app.js";

const schemaDef = {
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

export type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);

export type User = RowOf<typeof app.users>;
export type Project = RowOf<typeof app.projects>;
export type Todo = RowOf<typeof app.todos>;
