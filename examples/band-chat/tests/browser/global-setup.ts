import type { TestProject } from "vitest/node";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { APP_ID, ADMIN_SECRET } from "./test-constants.js";
let server: LocalJazzServerHandle | null = null;

export async function setup(project: TestProject) {
  server = await startLocalJazzServer({
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    inMemory: true,
    allowLocalFirstAuth: true,
  });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret!,
    schema: app,
    permissions,
  });
  project.provide("bandChatServerUrl", server.url);
}
export async function teardown() {
  await server?.stop();
}
