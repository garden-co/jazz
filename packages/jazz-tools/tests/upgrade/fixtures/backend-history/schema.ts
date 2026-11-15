import { schema as s } from "jazz-tools";
import { migrations } from "./migrations/index.js";

const schema = {
  notes: s.table({ title: s.string(), body: s.string(), pinned: s.boolean().default(false) }, {}),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema, { migrations });

export type Note = s.RowOf<typeof app.notes>;
