import { test, expect } from "@playwright/test";
import {
  LocalJazzServerHandle,
  pushSchemaCatalogue,
  startLocalJazzServer,
} from "jazz-tools/testing";
import { join } from "node:path";

function buildClientUrl(options: {
  appId: string;
  token: string;
  adminSecret?: string;
  serverUrl?: string;
  schemaVersion: "v1" | "v2";
}): string {
  const url = new URL(BASE_URL);
  url.searchParams.set("appId", options.appId);
  options.adminSecret && url.searchParams.set("adminSecret", options.adminSecret);
  url.searchParams.set("token", options.token);
  options.serverUrl && url.searchParams.set("serverUrl", options.serverUrl);
  url.searchParams.set("schemaVersion", options.schemaVersion);
  return url.toString();
}

const BASE_URL = "http://127.0.0.1:5173/cases/lens/index.html";
const APP_ID = "00000000-0000-0000-0000-000000000001";
const SYNC_PORT = 1625;
const ADMIN_SECRET = "lens-e2e-admin-secret";

let server: LocalJazzServerHandle;
test.beforeEach(async () => {
  server = await startLocalJazzServer({
    appId: APP_ID,
    port: SYNC_PORT,
    adminSecret: ADMIN_SECRET,
    enableLogs: true,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, "schema_v2"),
  });
});

test.afterEach(async () => {
  await server.stop();
});

test("v1 schema should be able to read data from v1 schema", async ({ browser }) => {
  const contextV1A = await browser.newContext();
  const contextV1B = await browser.newContext();
  const pageV1A = await contextV1A.newPage();
  const pageV1B = await contextV1B.newPage();

  await pageV1A.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "client-a",
      serverUrl: server.url,
      schemaVersion: "v1",
    }),
  );

  await pageV1B.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "client-b",
      serverUrl: server.url,
      schemaVersion: "v1",
    }),
  );

  await expect(pageV1A.getByRole("heading", { name: "App v1" })).toBeVisible();
  await expect(pageV1B.getByRole("heading", { name: "App v1" })).toBeVisible();

  const taskV1 = "first";
  await pageV1A.getByLabel("Todo title").fill(taskV1);
  await pageV1A.getByRole("button", { name: "Add todo" }).click();

  // Task is visible in V2
  await expect(pageV1B.locator("#todo-list li", { hasText: taskV1 })).toHaveCount(1);
  // Task is visible in V1
  await expect(pageV1A.locator("#todo-list li", { hasText: taskV1 })).toHaveCount(1);

  await contextV1A.close();
  await contextV1B.close();
});

test("v2 schema should be able to read data from v1 schema", async ({ browser }) => {
  const contextV1 = await browser.newContext();
  const contextV2 = await browser.newContext();
  const pageV1 = await contextV1.newPage();
  const pageV2 = await contextV2.newPage();

  await pageV1.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "client-a",
      serverUrl: server.url,
      schemaVersion: "v1",
    }),
  );

  await pageV2.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "client-b",
      serverUrl: server.url,
      schemaVersion: "v2",
    }),
  );

  await expect(pageV1.getByRole("heading", { name: "App v1" })).toBeVisible();
  await expect(pageV2.getByRole("heading", { name: "App v2" })).toBeVisible();

  const taskV1 = "first";
  await pageV1.getByLabel("Todo title").fill(taskV1);
  await pageV1.getByRole("button", { name: "Add todo" }).click();

  // Task is visible in V1
  await expect(pageV1.locator("#todo-list li", { hasText: taskV1 })).toHaveCount(1);
  // Task is visible in V2
  await expect(pageV2.locator("#todo-list li", { hasText: taskV1 })).toHaveCount(1);

  await contextV1.close();
  await contextV2.close();
});

test("v1 schema should be able to read data from v2 schema", async ({ browser }) => {
  const contextV1 = await browser.newContext();
  const contextV2 = await browser.newContext();
  const pageV1 = await contextV1.newPage();
  const pageV2 = await contextV2.newPage();

  await pageV1.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "client-a",
      serverUrl: server.url,
      schemaVersion: "v1",
    }),
  );

  await pageV2.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "client-b",
      serverUrl: server.url,
      schemaVersion: "v2",
    }),
  );

  await expect(pageV1.getByRole("heading", { name: "App v1" })).toBeVisible();
  await expect(pageV2.getByRole("heading", { name: "App v2" })).toBeVisible();

  const taskV2 = "second";
  await pageV2.getByLabel("Todo title").fill(taskV2);
  await pageV2.getByRole("button", { name: "Add todo" }).click();

  // Task is visible in V2
  await expect(pageV2.locator("#todo-list li", { hasText: taskV2 })).toHaveCount(1);
  // Task is visible in V1
  await expect(pageV1.locator("#todo-list li", { hasText: taskV2 })).toHaveCount(1);

  await contextV1.close();
  await contextV2.close();
});
