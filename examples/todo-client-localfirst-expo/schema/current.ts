import { table, col } from "jazz-tools";

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
});
