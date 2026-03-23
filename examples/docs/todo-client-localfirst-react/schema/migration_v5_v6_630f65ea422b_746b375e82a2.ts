import { migrate, col } from "jazz-tools";

migrate("todos", {
  parentId: col.drop().string({ backwardsDefault: null }),
  projectId: col.drop().string({ backwardsDefault: null }),
  description: col.drop().string({ backwardsDefault: "" }),
});

migrate("projects", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});
