import { col, defineApp, type DefinedSchema, type RowOf, type TypedApp } from "jazz-tools";

const schemaDef = {
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

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);

export type Todo = RowOf<typeof app.todos>;
