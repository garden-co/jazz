import { join } from "node:path";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, ADMIN_SECRET, APP_ID };

let server: TestingServer | null = null;
export async function setup(): Promise<void> {
  if (server) return;

  server = await TestingServer.start({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, "../../schema"),
  });
}

export async function teardown(): Promise<void> {
  await server?.stop();
}
