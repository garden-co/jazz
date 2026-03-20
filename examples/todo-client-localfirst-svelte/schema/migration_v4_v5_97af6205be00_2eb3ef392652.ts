import { migrate, col } from "jazz-tools";

migrate("todos", {
  parentId: col.rename("project"),
  ownerId: col.rename("owner_id"),
  projectId: col.rename("parent"),
});
