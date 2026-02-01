import { table, col } from "jazz-ts";

table("todos", {
  title: col.string(),
  done: col.boolean(),
  description: col.string().optional(),
});
