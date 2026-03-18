import { migrate, col } from "jazz-tools";

migrate("file_parts", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});

migrate("files", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});

migrate("instruments", {
  sound: col.drop().bytes({ backwardsDefault: new Uint8Array([]) }),
  soundFileId: col.add().optional().string({ default: null }),
});
