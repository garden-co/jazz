import type { TestProject } from "vitest/node";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { ADMIN_SECRET, APP_ID } from "./test-constants.js";

let server: LocalJazzServerHandle | null = null;

export async function setup(project: TestProject): Promise<void> {
  // Vitest can call a global setup more than once (once per browser project),
  // so start the server only on the first call and share its URL.
  if (!server) {
    server = await startLocalJazzServer({
      appId: APP_ID,
      adminSecret: ADMIN_SECRET,
      inMemory: true,
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: ADMIN_SECRET,
      schema: app,
      permissions,
    });
  }
  project.provide("jazzServerUrl", server.url);
}

export async function teardown(): Promise<void> {
  await server?.stop();
  server = null;
}
