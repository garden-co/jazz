import { schema as s } from "jazz-tools";

const schema = {
  folders: s.table({
    name: s.string(),
    owner_id: s.string(),
  }),
  files: s
    .table({
      folder_id: s.ref("folders"),
      name: s.string(),
      content_type: s.string(),
      size_bytes: s.int(),
      owner_id: s.string(),
      contents: s.bytes(),
    })
    // The browser always opens one folder at a time.
    .indexOnly(["folder_id"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
