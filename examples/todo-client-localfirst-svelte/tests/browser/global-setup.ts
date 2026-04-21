import { join } from "node:path";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, ADMIN_SECRET, APP_ID };

let server: Promise<TestingServer> | null = null;

export async function setup(): Promise<void> {
  if (server) {
    await server;
    return;
  }

  server = TestingServer.start({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

  const serverHandle = await server;

  await pushSchemaCatalogue({
    serverUrl: serverHandle.url,
    appId: serverHandle.appId,
    adminSecret: serverHandle.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, "../../src/lib"),
  });
}

export async function teardown(): Promise<void> {
  const s = await server;
  await s?.stop();
}
