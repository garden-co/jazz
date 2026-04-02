import { schema as s } from "jazz-tools";

s.migrate("file_parts", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});

s.migrate("files", {
  // TODO: Table-level operation not yet supported in TypeScript DSL
});

s.migrate("instruments", {
  sound: s.drop.bytes({ backwardsDefault: new Uint8Array([]) }),
  soundFileId: s.add.string({ default: null }),
});
