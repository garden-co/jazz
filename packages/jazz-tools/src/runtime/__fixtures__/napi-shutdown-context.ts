import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createJazzContext } from "../../backend/create-jazz-context.js";
import { schema as s } from "../../index.js";

const dataPath = await mkdtemp(join(tmpdir(), "jazz-napi-exit-"));
const app = s.defineApp({
  items: s.table({ text: s.string() }),
});
const context = createJazzContext({
  app,
  appId: "napi-exit-test",
  driver: {
    dataPath: join(dataPath, "db.jazz"),
    type: "persistent",
  },
  permissions: {},
});

try {
  context.db();
  await context.shutdown();
  console.log("shutdown complete");
} finally {
  await rm(dataPath, { force: true, recursive: true });
}
