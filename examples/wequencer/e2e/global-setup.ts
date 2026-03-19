/**
 * Playwright global setup — spawns a local jazz-tools server for sync tests.
 */

import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { startLocalJazzServer, pushSchemaCatalogue } from "jazz-tools/testing";

const SERVER_PORT = 19878;
const ADMIN_SECRET = "wequencer-test-admin-secret";
const APP_ID = "00000000-0000-0000-0000-000000000099";

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  const server = await startLocalJazzServer({
    appId: APP_ID,
    port: SERVER_PORT,
    adminSecret: ADMIN_SECRET,
    enableLogs: true,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, ".."),
  });

  return () => server.stop();
}

export default globalSetup;
