import { table, col } from "jazz-tools";

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  owner_id: col.string(),
  parent_id: col.ref("todos").optional(),
  project_id: col.ref("projects").optional(),
});
