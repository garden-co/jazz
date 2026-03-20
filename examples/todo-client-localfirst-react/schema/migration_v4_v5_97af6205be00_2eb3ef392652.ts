import { migrate, col } from "jazz-tools";

migrate("todos", {
  parentId: col.rename("parent"),
  ownerId: col.rename("owner_id"),
  projectId: col.rename("project"),
});
