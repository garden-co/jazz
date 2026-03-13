import { migrate, col } from "jazz-tools";

migrate("file_parts", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});

migrate("todos", {
  owner_id: col.add().string({ default: "" }),
});

migrate("files", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});

migrate("uploads", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});
