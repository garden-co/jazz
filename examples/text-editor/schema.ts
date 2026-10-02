import { schema as s } from "jazz-tools";

const schema = {
  documents: s.table({ contentLog: s.bytes() }, {}),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
