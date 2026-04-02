import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import { DEFAULT_ADMIN_SECRET, DEFAULT_APP_ID, WORKOS_CLIENT_ID } from "../constants.js";

async function globalSetup(): Promise<() => Promise<void>> {
  const jazzServer = await TestingServer.start({
    appId: DEFAULT_APP_ID,
    port: 1625,
    adminSecret: DEFAULT_ADMIN_SECRET,
    jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`,
  });

  await pushSchemaCatalogue({
    serverUrl: jazzServer.url,
    appId: DEFAULT_APP_ID,
    adminSecret: DEFAULT_ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, ".."),
  });

  return async () => {
    await jazzServer.stop();
  };
}

export default globalSetup;
