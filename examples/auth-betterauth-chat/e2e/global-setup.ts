import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";

const APP_ID = process.env.NEXT_PUBLIC_APP_ID!;
const ADMIN_SECRET = process.env.ADMIN_SECRET!;
const BACKEND_SECRET = process.env.BACKEND_SECRET!;
const APP_ORIGIN = process.env.NEXT_PUBLIC_APP_ORIGIN!;

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  console.log(`JWKS URL: ${APP_ORIGIN}/api/auth/jwks`);

  const server = await TestingServer.start({
    appId: APP_ID,
    port: 1625,
    adminSecret: ADMIN_SECRET,
    backendSecret: BACKEND_SECRET,
    jwksUrl: `${APP_ORIGIN}/api/auth/jwks`,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: server.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, "../schema-better-auth"),
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: server.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, "../schema"),
  });

  return async () => {
    await server.stop();
  };
}

export default globalSetup;
