import { deploy, startLocalJazzServer } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { ADMIN_SECRET, APP_ID, TEST_PORT } from "./test-constants.js";

let server: Awaited<ReturnType<typeof startLocalJazzServer>> | null = null;

export async function setup(): Promise<void> {
  server = await startLocalJazzServer({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
    inMemory: true,
  });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret!,
    schema: app,
    permissions,
  });
}

export async function teardown(): Promise<void> {
  await server?.stop();
  server = null;
}
