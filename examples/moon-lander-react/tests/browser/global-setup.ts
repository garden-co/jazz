/**
 * Global setup for browser tests — spawns a real jazz server on a random port.
 *
 * The port is chosen by vitest.config.browser.ts and passed via process.env.
 * The schema is pushed explicitly so clients don't need adminSecret.
 */

import { join } from "node:path";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";

const TEST_PORT = parseInt(process.env.TEST_PORT!, 10);
const ADMIN_SECRET = "test-admin-secret-for-moon-lander-tests";
const APP_ID = "00000000-0000-0000-0000-000000000003";
const APP_ID_MULTI = "00000000-0000-0000-0000-000000000004";

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

  const handle = await server;

  const schemaDir = join(import.meta.dirname ?? __dirname, "../../schema");
  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir,
  });
  // Register schema for the isolated multi-player namespace so test 837
  // starts with an empty event history (no stream connect timeout).
  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: APP_ID_MULTI,
    adminSecret: ADMIN_SECRET,
    schemaDir,
  });
}

export async function teardown(): Promise<void> {
  await (await server)?.stop();
}
