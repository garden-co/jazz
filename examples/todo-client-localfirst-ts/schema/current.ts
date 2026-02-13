import { table, col } from "jazz-ts";

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
  parent: col.ref("todos").optional(),
  project: col.ref("projects"),
});
