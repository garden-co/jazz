import { schema as s } from "jazz-tools";

const schema = {
  files: s.table({
    name: s.string().optional(),
    mime_type: s.string(),
    data: s.bytes(),
  }),
  uploads: s.table({
    size: s.int(),
    lastModified: s.timestamp(),
    fileId: s.ref("files"),
    owner_id: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

const uploadWithPartsQuery = app.uploads
  .include({
    file: true,
  })
  .requireIncludes();

export type File = s.RowOf<typeof app.files>;
export type UploadWithIncludes = s.RowOf<typeof uploadWithPartsQuery>;
