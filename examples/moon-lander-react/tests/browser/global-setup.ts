/**
 * Global setup for browser tests — spawns a real jazz server on a random port.
 *
 * The port is chosen by vitest.config.browser.ts and passed via process.env.
 * The schema is pushed explicitly so clients don't need adminSecret.
 */

import type { TestProject } from "vitest/node";
import { startLocalJazzServer, deploy, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";

const ADMIN_SECRET = "test-admin-secret-for-moon-lander-tests";
const APP_ID = "00000000-0000-0000-0000-000000000003";

let server: LocalJazzServerHandle | null = null;

export async function setup(project: TestProject): Promise<void> {
  if (server) return;

  server = await startLocalJazzServer({
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
  });
  project.provide("jazzServerUrl", server.url);

  await deploy({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schema: app,
    permissions,
  });
}

export async function teardown(): Promise<void> {
  await server?.stop();
}
