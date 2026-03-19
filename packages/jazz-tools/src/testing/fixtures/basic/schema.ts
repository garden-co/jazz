import { col } from "../../../dsl.js";
import { defineApp, type DefinedSchema, type TypedApp } from "../../../typed-app.js";

const schemaDef = {
  todos: {
    title: col.string(),
    done: col.boolean(),
  },
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);
