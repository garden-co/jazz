import { table, col } from "jazz-tools";

table("projects", {
  name: col.string(),
  owner_id: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  owner_id: col.string(),
  parent: col.ref("todos").optional(),
  project: col.ref("projects").optional(),
});
