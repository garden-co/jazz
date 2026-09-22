import { schema as s } from "jazz-tools";

const schema = {
  folders: s.table(
    {
      name: s.string(),
      owner_id: s.uuid(),
    },
    { filesViaFolder: s.reverse("files", "folder") },
  ),
  files: s
    .table(
      {
        folder_id: s.uuid(),
        name: s.string(),
        content_type: s.string(),
        size_bytes: s.int(),
        owner_id: s.uuid(),
        contents: s.bytes(),
      },
      { folder: s.rel("folders", "folder_id") },
    )
    // The browser always opens one folder at a time.
    .indexOnly(["folder_id"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
