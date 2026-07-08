import { startLocalJazzServer, deploy, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, ADMIN_SECRET, APP_ID };

let server: LocalJazzServerHandle | null = null;
export async function setup(): Promise<void> {
  if (server) return;

  server = await startLocalJazzServer({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

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
