import { table, col } from "jazz-tools";

// #region schema-todo-client-ts
table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  ownerId: col.string().optional(),
  parentId: col.ref("todos").optional(),
  projectId: col.ref("projects").optional(),
});
// #endregion schema-todo-client-ts

// #region schema-files-and-blobs-ts
table("file_parts", {
  data: col.bytes(),
});

table("files", {
  name: col.string().optional(),
  mimeType: col.string(),
  partIds: col.array(col.ref("file_parts")),
  partSizes: col.array(col.int()),
});

table("uploads", {
  ownerId: col.string(),
  label: col.string(),
  fileId: col.ref("files"),
});
// #endregion schema-files-and-blobs-ts
