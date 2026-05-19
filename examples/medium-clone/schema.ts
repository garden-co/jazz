import { schema as s } from "jazz-tools";

const schema = {
  articles: s.table({
    title: s.string(),
    subtitle: s.string(),
    content: s.string(),
    labels: s.array(s.string()),
    authorId: s.string(),
    published: s.boolean(),
    createdAt: s.timestamp(),
    // Optional cover image. When unset, the UI falls back to a gradient
    // generated from the title.
    coverImageId: s.ref("files").optional(),
  }),
  // One row per (article, user) draft. The row id doubles as the branch id
  // passed to db.createBranch / db.branch.
  drafts: s.table({
    articleId: s.ref("articles"),
    ownerId: s.string(),
    createdAt: s.timestamp(),
  }),
  // The two tables below are the conventional file-storage pair Jazz expects:
  // `Db.createFileFromBlob` writes parts into `file_parts` and a header row
  // into `files`.
  files: s.table({
    name: s.string(),
    mimeType: s.string(),
    partIds: s.array(s.ref("file_parts")),
    partSizes: s.array(s.int()),
  }),
  file_parts: s.table({
    data: s.bytes(),
  }),
  // Tracks which user uploaded which file, so deletes can be gated to the
  // original uploader without giving them ownership of every file in storage.
  image_uploads: s.table({
    fileId: s.ref("files"),
    ownerId: s.string(),
    createdAt: s.timestamp(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Article = s.RowOf<typeof app.articles>;
export type Draft = s.RowOf<typeof app.drafts>;
