import { table, col } from "jazz-tools";

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  tags: col.array(col.string()),
  project: col.ref("projects"),
});
