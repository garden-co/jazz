import { table, col } from "jazz-tools";

table("todos", {
  title: col.string(),
  done: col.boolean(),
  owner_id: col.string(),
});
