import { col, defineApp, type Schema, type RowOf, type App } from "jazz-tools";

const schema = {
  files: {
    name: col.string(),
    mimeType: col.string(),
    partIds: col.array(col.ref("file_parts")),
    partSizes: col.array(col.int()),
  },
  file_parts: {
    data: col.bytes(),
  },
  uploads: {
    size: col.int(),
    lastModified: col.timestamp(),
    fileId: col.ref("files"),
    ownerId: col.string(),
  },
};

type AppSchema = Schema<typeof schema>;
export const app: App<AppSchema> = defineApp(schema);

const uploadWithPartsQuery = app.uploads
  .include({
    file: {
      parts: true,
    },
  })
  .requireIncludes();

export type File = RowOf<typeof app.files>;
export type UploadWithIncludes = RowOf<typeof uploadWithPartsQuery>;
