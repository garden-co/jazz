import { col, defineApp, type DefinedSchema, type RowOf, type TypedApp } from "jazz-tools";

const schemaDef = {
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

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);

const uploadWithPartsQuery = app.uploads
  .include({
    file: {
      parts: true,
    },
  })
  .requireIncludes();

export type File = RowOf<typeof app.files>;
export type UploadWithIncludes = RowOf<typeof uploadWithPartsQuery>;
