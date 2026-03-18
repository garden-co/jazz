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
  parentId: col.ref("todos").optional(),
  projectId: col.ref("projects").optional(),
});
// #endregion schema-expo
