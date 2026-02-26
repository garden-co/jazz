import {
  type LocalJazzServerHandle,
  startLocalJazzServer,
} from "../../src/testing/local-jazz-server.js";
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

  await server;
}

export async function teardown(): Promise<void> {
  await (await server)?.stop();
}
