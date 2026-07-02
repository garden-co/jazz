import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import type { TestProject } from "vitest/node";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { ADMIN_SECRET, APP_ID } from "./test-constants.js";

export async function setup(project: TestProject): Promise<() => Promise<void>> {
  const handle = await startLocalJazzServer({
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    inMemory: true,
  });

  await deploy({
    serverUrl: handle.url,
    appId: handle.appId,
    adminSecret: ADMIN_SECRET,
    schema: app,
    permissions,
  });

  project.provide("worldTourJazzServerUrl", handle.url);

  return async () => {
    await handle.stop();
  };
}
