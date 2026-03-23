import { migrate, col } from "jazz-tools";

migrate("todos", {
  description: col.drop().string({ backwardsDefault: "" }),
  parentId: col.drop().string({ backwardsDefault: null }),
  projectId: col.drop().string({ backwardsDefault: null }),
});

migrate("projects", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});
