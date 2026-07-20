import { join } from "node:path";
import { startLocalJazzServer, deploy } from "jazz-tools/testing";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID };

type LocalServer = Awaited<ReturnType<typeof startLocalJazzServer>>;

let server: LocalServer | null = null;
let setupPromise: Promise<void> | null = null;

export async function setup(): Promise<void> {
  if (!setupPromise) {
    setupPromise = (async () => {
      server = await startLocalJazzServer({
        appId: APP_ID,
        port: TEST_PORT,
        adminSecret: ADMIN_SECRET,
        inMemory: true,
      });

      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret!,
        schemaDir: join(import.meta.dirname, "../.."),
      });
    })();
  }

  await setupPromise;
}

export async function teardown(): Promise<void> {
  await server?.stop();
  server = null;
  setupPromise = null;
}
