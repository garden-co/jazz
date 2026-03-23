// #region schema-expo
import { table, col } from "jazz-tools";

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  ownerId: col.string(),
  projectId: col.ref("projects").optional(),
  parentId: col.ref("todos").optional(),
});
// #endregion schema-expo
