import { schema as s } from "jazz-tools";

const schema = {
  documents: s.table({ title: s.string() }, {}),
  documentLogs: s.table(
    { documentId: s.uuid(), contentLog: s.bytes() },
    { document: s.rel("documents", "documentId") },
  ),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
