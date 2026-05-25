import { join } from "node:path";
import { pushSchemaCatalogue, startLocalJazzServer } from "jazz-tools/testing";
import type { TestProject } from "vitest/node";
import { ADMIN_SECRET, APP_ID } from "./test-constants.js";

export async function setup(project: TestProject): Promise<() => Promise<void>> {
  const handle = await startLocalJazzServer({
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    inMemory: true,
  });

  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: handle.appId,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname, "../.."),
  });

  project.provide("worldTourJazzServerUrl", handle.url);

  return async () => {
    await handle.stop();
  };
}
