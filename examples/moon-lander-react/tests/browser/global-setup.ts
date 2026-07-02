/**
 * Global setup for browser tests — spawns a real jazz server on a random port.
 *
 * The port is chosen by vitest.config.browser.ts and passed via process.env.
 * The schema is pushed explicitly so clients don't need adminSecret.
 */

import { startLocalJazzServer, deploy, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";

const TEST_PORT = parseInt(process.env.TEST_PORT!, 10);
const ADMIN_SECRET = "test-admin-secret-for-moon-lander-tests";
const APP_ID = "00000000-0000-0000-0000-000000000003";

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

  await deploy({
    serverUrl: handle.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schema: app,
    permissions,
  });
}

export async function teardown(): Promise<void> {
  await (await server)?.stop();
}
