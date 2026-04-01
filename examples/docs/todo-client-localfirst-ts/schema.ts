import { schema as s } from "jazz-tools";

const schema = {
  // #region schema-todo-client-ts
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    ownerId: s.string().optional(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional(),
  }),
  // #endregion schema-todo-client-ts

  // #region schema-files-and-blobs-ts
  file_parts: s.table({
    data: s.bytes(),
  }),
  files: s.table({
    name: s.string().optional(),
    mimeType: s.string(),
    partIds: s.array(s.ref("file_parts")),
    partSizes: s.array(s.int()),
  }),
  uploads: s.table({
    ownerId: s.string(),
    label: s.string(),
    fileId: s.ref("files"),
  }),
  // #endregion schema-files-and-blobs-ts
};

// #region schema-define-app-ts
type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Todo = s.RowOf<typeof app.todos>;
// #endregion schema-define-app-ts
