/**
 * Global setup for browser tests — spawns a real jazz server on a random port.
 */

import { join } from "node:path";
import {
  type LocalJazzServerHandle,
  startLocalJazzServer,
  pushSchemaCatalogue,
} from "jazz-tools/testing";
import { ADMIN_SECRET, APP_ID } from "./test-constants";

const TEST_PORT = parseInt(process.env.TEST_PORT!, 10);

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

  const handle = await server;

  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, "../../schema"),
  });
}

export async function teardown(): Promise<void> {
  await (await server)?.stop();
}
