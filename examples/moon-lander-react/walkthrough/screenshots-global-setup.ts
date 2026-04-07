/**
 * Global setup for Playwright screenshot capture.
 * Starts a dedicated Jazz server on a fixed port so the test can navigate
 * to the app with URL params (appId, serverUrl) that point at it.
 *
 * Run via: pnpm walkthrough:shots
 */

import { join } from "node:path";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";

export const SCREENSHOT_PORT = 4201;
export const SCREENSHOT_APP_ID = "00000000-0000-0000-0000-000000000006";
const ADMIN_SECRET = "screenshot-admin-secret-moon-lander";

export default async function globalSetup(): Promise<() => Promise<void>> {
  const server = await TestingServer.start({
    appId: SCREENSHOT_APP_ID,
    port: SCREENSHOT_PORT,
    adminSecret: ADMIN_SECRET,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: SCREENSHOT_APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, ".."),
  });

  return () => server.stop();
}
