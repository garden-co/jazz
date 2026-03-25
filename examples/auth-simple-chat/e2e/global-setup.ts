import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import { AUTH_JWT_KID, DEFAULT_ADMIN_SECRET, DEFAULT_APP_ID } from "../constants.js";
import { startAuthServer } from "../server/auth-server.js";

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  const authServer = await startAuthServer({
    port: 3001,
    jwtKid: AUTH_JWT_KID,
  });

  const jazzServer = await TestingServer.start({
    appId: DEFAULT_APP_ID,
    port: 1625,
    adminSecret: DEFAULT_ADMIN_SECRET,
    jwksUrl: `${authServer.url}/.well-known/jwks.json`,
  });

  await pushSchemaCatalogue({
    serverUrl: jazzServer.url,
    appId: DEFAULT_APP_ID,
    adminSecret: DEFAULT_ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, "../schema"),
  });

  return async () => {
    await authServer.stop();
    await jazzServer.stop();
  };
}

export default globalSetup;
