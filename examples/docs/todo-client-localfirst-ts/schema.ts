import { schema as s } from "jazz-tools";

const schema = {
  // #region schema-todo-client-ts
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    priority: s.int().optional(),
    description: s.string().optional(),
    owner_id: s.string().optional(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional(),
  }),
  // #endregion schema-todo-client-ts

  // #region schema-files-and-blobs-ts
  files: s.table({
    name: s.string().optional(),
    mime_type: s.string(),
    data: s.bytes(),
  }),
  uploads: s.table({
    owner_id: s.string(),
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
