import { col, defineApp, type DefinedSchema, type RowOf, type TypedApp } from "jazz-tools";

const schemaDef = {
  // #region schema-todo-client-ts
  projects: {
    name: col.string(),
  },
  todos: {
    title: col.string(),
    done: col.boolean(),
    description: col.string().optional(),
    ownerId: col.string().optional(),
    parentId: col.ref("todos").optional(),
    projectId: col.ref("projects").optional(),
  },
  // #endregion schema-todo-client-ts

  // #region schema-files-and-blobs-ts
  file_parts: {
    data: col.bytes(),
  },
  files: {
    name: col.string().optional(),
    mimeType: col.string(),
    partIds: col.array(col.ref("file_parts")),
    partSizes: col.array(col.int()),
  },
  uploads: {
    ownerId: col.string(),
    label: col.string(),
    fileId: col.ref("files"),
  },
  // #endregion schema-files-and-blobs-ts
};

type AppSchema = DefinedSchema<typeof schemaDef>;
export const app: TypedApp<AppSchema> = defineApp(schemaDef);

export type Todo = RowOf<typeof app.todos>;
