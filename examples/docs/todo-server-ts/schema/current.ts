import { table, col } from "jazz-tools";

table("projects", {
  name: col.string(),
  owner_id: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  parentId: col.ref("todos").optional(),
  projectId: col.ref("projects").optional(),
  owner_id: col.string(),
});

table("todoShares", {
  todoId: col.ref("todos"),
  user_id: col.string(),
  can_read: col.boolean(),
});
