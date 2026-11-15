import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,
  messages: s.table({ text: s.string() }, {}),
};

export const app: s.App<s.Schema<typeof schema>> = s.defineApp(schema, { migrations });
