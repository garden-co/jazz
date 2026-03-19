import { join } from "node:path";
import {
  type LocalJazzServerHandle,
  startLocalJazzServer,
  pushSchemaCatalogue,
} from "jazz-tools/testing";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID };

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
    healthTimeoutMs: 5000,
  });

  const serverHandle = await server;

  await pushSchemaCatalogue({
    serverUrl: serverHandle.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, "../.."),
  });
}

export async function teardown(): Promise<void> {
  await (await server)?.stop();
}
