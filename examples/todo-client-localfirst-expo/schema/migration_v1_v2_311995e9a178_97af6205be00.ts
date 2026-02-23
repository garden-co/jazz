import { migrate, col } from "jazz-tools";

migrate("todos", {
  parent: col.add().optional().string({ default: null }),
  owner_id: col.add().string({ default: "" }),
  project: col.add().optional().string({ default: null }),
});

migrate("projects", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});
