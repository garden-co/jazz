import { col, defineApp, type DefinedSchema, type TypedApp } from "jazz-tools";

const schemaDef = {
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

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);
