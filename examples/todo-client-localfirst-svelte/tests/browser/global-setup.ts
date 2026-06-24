import { join } from "node:path";
import { startLocalJazzServer, deploy, type LocalJazzServerHandle } from "jazz-tools/testing";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, ADMIN_SECRET, APP_ID };

let server: Promise<LocalJazzServerHandle> | null = null;

export async function setup(): Promise<void> {
  if (server) {
    await server;
    return;
  }

  server = startLocalJazzServer({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

  const serverHandle = await server;

  await deploy({
    serverUrl: serverHandle.url,
    appId: serverHandle.appId,
    adminSecret: serverHandle.adminSecret,
    schemaDir: join(import.meta.dirname, "../../src/lib"),
  });
}

export async function teardown(): Promise<void> {
  const s = await server;
  await s?.stop();
}
