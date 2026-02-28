import { migrate, col } from "jazz-tools";

migrate("todos", {
  project: col.add().optional().string({ default: null }),
  parent: col.add().optional().string({ default: null }),
});

migrate("projects", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});
