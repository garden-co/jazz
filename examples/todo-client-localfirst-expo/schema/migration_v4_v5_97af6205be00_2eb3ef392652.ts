import { migrate, col } from "jazz-tools";

migrate("todos", {
  ownerId: col.rename("owner_id"),
  projectId: col.rename("parent"),
  parentId: col.rename("project"),
});
