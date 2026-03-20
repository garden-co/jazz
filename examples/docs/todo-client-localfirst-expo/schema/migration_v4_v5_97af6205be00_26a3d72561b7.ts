import { migrate, col } from "jazz-tools";

migrate("todos", {
  parentId: col.rename("parent"),
  projectId: col.rename("project"),
  ownerId: col.rename("owner_id"),
});
