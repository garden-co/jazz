import { table, col } from "jazz-tools";

table("users", {
  name: col.string(),
  friendsIds: col.array(col.ref("users")),
});

table("projects", {
  name: col.string(),
});

table("todos", {
  title: col.string(),
  done: col.boolean(),
  tags: col.array(col.string()),
  projectId: col.ref("projects"),
  ownerId: col.ref("users").optional(),
  assigneesIds: col.array(col.ref("users")),
});
