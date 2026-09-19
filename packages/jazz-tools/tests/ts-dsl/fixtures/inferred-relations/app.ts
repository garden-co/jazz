import { schema as s } from "jazz-tools";
import { schema, type AppSchema } from "./schema.js";

export const app: s.App<AppSchema> = s.defineApp(schema);
