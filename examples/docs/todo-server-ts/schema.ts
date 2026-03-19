import { col, defineApp, type DefinedSchema, type TypedApp } from "jazz-tools";

const schemaDef = {
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    done: col.boolean(),
    description: col.string().optional(),
    parentId: col.ref("todos").optional(),
    projectId: col.ref("projects").optional(),
    owner_id: col.string(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);
