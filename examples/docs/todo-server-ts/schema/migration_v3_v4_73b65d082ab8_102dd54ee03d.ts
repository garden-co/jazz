import { migrate, col } from "jazz-tools";

migrate("todos", {
  projectId: col.rename("project"),
  parentId: col.rename("parent"),
  description: col.drop().string({ backwardsDefault: "" }),
});

migrate("projects", {
  owner_id: col.add().string({ default: "" }),
});
