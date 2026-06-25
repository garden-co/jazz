/**
 * Playwright global setup — spawns a local jazz-tools server for sync tests.
 */

import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { startLocalJazzServer, deploy } from "jazz-tools/testing";

const SERVER_PORT = 19878;
const APP_ID = "00000000-0000-0000-0000-000000000099";

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  const server = await startLocalJazzServer({
    appId: APP_ID,
    port: SERVER_PORT,
  });

  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schemaDir: join(import.meta.dirname, ".."),
  });

  return () => server.stop();
}

export default globalSetup;
