import { schema as s } from "jazz-tools";

const schema = {
  organizations: s
    .table({
      name: s.string(),
      createdBy: s.string(),
    })
    .encryptionSpace(),
  members: s.table({
    orgId: s.ref("organizations"),
    userId: s.string(),
    role: s.string(),
  }),
  document_parts: s.table({
    orgId: s.ref("organizations"),
    data: s.bytes().encrypted("orgId"),
  }),
  documents: s.table({
    orgId: s.ref("organizations"),
    title: s.string().encrypted("orgId"),
    filename: s.string().encrypted("orgId"),
    mimeType: s.string(),
    size: s.int(),
    createdAt: s.timestamp(),
    uploadedBy: s.string(),
    partIds: s.array(s.ref("document_parts")),
    partSizes: s.array(s.int()),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

const documentsWithPartsQuery = app.documents
  .include({
    parts: true,
  })
  .requireIncludes();

export type Organization = s.RowOf<typeof app.organizations>;
export type Member = s.RowOf<typeof app.members>;
export type DocumentPart = s.RowOf<typeof app.document_parts>;
export type Document = s.RowOf<typeof app.documents>;
export type DocumentWithParts = s.RowOf<typeof documentsWithPartsQuery>;
