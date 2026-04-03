import { schema as s } from "jazz-tools";

const schema = {
  files: s.table({
    name: s.string(),
    mimeType: s.string(),
    partIds: s.array(s.ref("file_parts")),
    partSizes: s.array(s.int()),
  }),
  file_parts: s.table({
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
    file: {
      parts: true,
    },
  })
  .requireIncludes();

export type File = s.RowOf<typeof app.files>;
export type UploadWithIncludes = s.RowOf<typeof uploadWithPartsQuery>;
