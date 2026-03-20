import { migrate, col } from "jazz-tools";

migrate("todos", {
  parentId: col.rename("project"),
  projectId: col.rename("parent"),
});
