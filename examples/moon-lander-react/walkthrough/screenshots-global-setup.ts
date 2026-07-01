/**
 * Global setup for Playwright screenshot capture.
 * Starts a dedicated Jazz server on a fixed port so the test can navigate
 * to the app with URL params (appId, serverUrl) that point at it.
 *
 * Run via: pnpm walkthrough:shots
 */

import { startLocalJazzServer, deploy } from "jazz-tools/testing";
import permissions from "../permissions.js";
import { app } from "../schema.js";

export const SCREENSHOT_PORT = 4201;
export const SCREENSHOT_APP_ID = "00000000-0000-0000-0000-000000000006";
const ADMIN_SECRET = "screenshot-admin-secret-moon-lander";

export default async function globalSetup(): Promise<() => Promise<void>> {
  const server = await startLocalJazzServer({
    appId: SCREENSHOT_APP_ID,
    port: SCREENSHOT_PORT,
    adminSecret: ADMIN_SECRET,
  });

  await deploy({
    serverUrl: server.url,
    appId: SCREENSHOT_APP_ID,
    adminSecret: ADMIN_SECRET,
    schema: app,
    permissions,
  });

  return () => server.stop();
}
