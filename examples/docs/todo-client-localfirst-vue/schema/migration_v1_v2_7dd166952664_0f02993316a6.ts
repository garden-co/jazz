import { migrate, col } from "jazz-tools";

migrate("todos", {
  projectId: col.rename("project"),
  parentId: col.rename("parent"),
});
