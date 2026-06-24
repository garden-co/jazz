import { join } from "node:path";
import { startLocalJazzServer, deploy, type LocalJazzServerHandle } from "jazz-tools/testing";
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
    schemaDir: join(import.meta.dirname, "../.."),
  });
}

export async function teardown(): Promise<void> {
  await server?.stop();
}
