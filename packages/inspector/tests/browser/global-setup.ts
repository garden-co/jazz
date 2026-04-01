import { join } from "node:path";
import { JazzClient } from "jazz-tools";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";
import { ADMIN_SECRET, APP_ID, TEST_BRANCH, TEST_ENV, TEST_PORT } from "./test-constants.js";
import { app } from "./schema.ts";

export default async function globalSetup(): Promise<() => Promise<void>> {
  const serverHandlePromise = TestingServer.start({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

  const serverHandle = await serverHandlePromise;
  await pushSchemaCatalogue({
    serverUrl: serverHandle.url,
    appId: serverHandle.appId,
    adminSecret: serverHandle.adminSecret,
    env: TEST_ENV,
    userBranch: TEST_BRANCH,
    schemaDir: join(import.meta.dirname ?? __dirname, "."),
  });

  const client = await JazzClient.connect({
    appId: APP_ID,
    schema: app.wasmSchema,
    serverUrl: serverHandle.url,
    env: TEST_ENV,
    userBranch: TEST_BRANCH,
    adminSecret: ADMIN_SECRET,
    localAuthMode: "demo",
    localAuthToken: "test-token",
  });

  await client.createWithAck(
    "todos",
    [
      { type: "Text", value: "First seeded todo" },
      { type: "Boolean", value: false },
    ],
    "edge",
  );

  await client.createWithAck(
    "todos",
    [
      { type: "Text", value: "Second seeded todo" },
      { type: "Boolean", value: true },
    ],
    "edge",
  );

  await client.shutdown();

  return async () => {
    await serverHandle.stop();
  };
}
