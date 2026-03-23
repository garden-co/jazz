import { migrate, col } from "jazz-tools";

migrate("todos", {
  projectId: col.rename("project"), // TODO: Review this auto-generated operation
  ownerId: col.rename("owner_id"), // TODO: Review this auto-generated operation
  parentId: col.rename("parent"), // TODO: Review this auto-generated operation
});
