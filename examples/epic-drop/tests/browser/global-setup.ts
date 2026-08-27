import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { ADMIN_SECRET, APP_ID, TEST_PORT } from "./test-constants.js";

let server: Promise<LocalJazzServerHandle> | null = null;

export async function setup(): Promise<void> {
  if (!server) {
    server = startLocalJazzServer({
      appId: APP_ID,
      port: TEST_PORT,
      adminSecret: ADMIN_SECRET,
      inMemory: true,
    });
  }
  const active = await server;
  await deploy({
    serverUrl: active.url,
    appId: active.appId,
    adminSecret: active.adminSecret!,
    schema: app,
    permissions,
  });
}

export async function teardown(): Promise<void> {
  const active = await server;
  await active?.stop();
  server = null;
}
