import { col, defineApp, type DefinedSchema, type TypedApp } from "jazz-tools";

const schemaDef = {
  todos: {
    title: col.string(),
    done: col.boolean(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;

export const app: TypedApp<AppSchema> = defineApp(schemaDef);
