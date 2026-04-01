/**
 * Playwright global setup — spawns a local jazz-tools server for sync tests.
 */

import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";

const SERVER_PORT = 19878;
const APP_ID = "00000000-0000-0000-0000-000000000099";

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  const server = await TestingServer.start({
    appId: APP_ID,
    port: SERVER_PORT,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, ".."),
  });

  return () => server.stop();
}

export default globalSetup;
