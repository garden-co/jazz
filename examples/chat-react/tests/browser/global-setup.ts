import type { TestProject } from "vitest/node";
import { startLocalJazzServer, deploy, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { ADMIN_SECRET, APP_ID };

let server: LocalJazzServerHandle | null = null;
export async function setup(project: TestProject): Promise<void> {
  if (server) return;

  server = await startLocalJazzServer({
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
  });
  process.env.VITE_JAZZ_TEST_SERVER_URL = server.url;
  project.provide("jazzServerUrl", server.url);

  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema: app,
    permissions,
  });
}

export async function teardown(): Promise<void> {
  await server?.stop();
}
