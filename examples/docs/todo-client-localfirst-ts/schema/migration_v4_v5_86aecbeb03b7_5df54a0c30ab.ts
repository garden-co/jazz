import { migrate, col } from "jazz-tools";

migrate("files", {
  partIds: col.rename("parts"),
});

migrate("uploads", {
  ownerId: col.rename("owner_id"),
  fileId: col.rename("file"),
});

migrate("todos", {
  parentId: col.rename("project"),
  projectId: col.rename("parent"),
  ownerId: col.rename("owner_id"),
});
