import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import { APP_ORIGIN, DEFAULT_ADMIN_SECRET, DEFAULT_APP_ID } from "../constants";

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  console.log(`JWKS URL: ${APP_ORIGIN}/api/auth/jwks`);

  const jazzServer = await TestingServer.start({
    appId: DEFAULT_APP_ID,
    port: 1625,
    adminSecret: DEFAULT_ADMIN_SECRET,
    jwksUrl: `${APP_ORIGIN}/api/auth/jwks`,
  });

  await pushSchemaCatalogue({
    serverUrl: jazzServer.url,
    appId: DEFAULT_APP_ID,
    adminSecret: DEFAULT_ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, "../schema"),
  });

  return async () => {
    await jazzServer.stop();
  };
}

export default globalSetup;
