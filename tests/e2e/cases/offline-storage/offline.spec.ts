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
}): string {
  const url = new URL(BASE_URL);
  url.searchParams.set("appId", options.appId);
  options.adminSecret && url.searchParams.set("adminSecret", options.adminSecret);
  url.searchParams.set("token", options.token);
  options.serverUrl && url.searchParams.set("serverUrl", options.serverUrl);
  return url.toString();
}

const BASE_URL = "http://127.0.0.1:5173/cases/offline-storage/index.html";
const APP_ID = "00000000-0000-0000-0000-000000000001";
const SYNC_PORT = 1625;
const ADMIN_SECRET = "offline-e2e-admin-secret";

let server: LocalJazzServerHandle;
test.beforeEach(async ({ page }) => {
  server = await startLocalJazzServer({
    appId: APP_ID,
    port: SYNC_PORT,
    adminSecret: ADMIN_SECRET,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, "schema"),
  });
});

test.afterEach(async () => {
  await server.stop();
});

test("writes locally while offline and converges after reconnect", async ({ browser }) => {
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();

  await pageA.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "offline-client-a",
      serverUrl: server.url,
    }),
  );

  await pageB.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "offline-client-b",
      serverUrl: server.url,
    }),
  );

  await expect(pageA.getByRole("heading", { name: "Offline App" })).toBeVisible();
  await expect(pageB.getByRole("heading", { name: "Offline App" })).toBeVisible();

  // Establish baseline connectivity before forcing offline behavior.
  const warmupTitle = "warmup-online-task";
  await pageA.getByLabel("Todo title").fill(warmupTitle);
  await pageA.getByRole("button", { name: "Add todo" }).click();
  await expect(pageB.locator("#todo-list li", { hasText: warmupTitle })).toHaveCount(1, {
    timeout: 20_000,
  });

  const offlineTitle = "offline-first-task";

  await contextA.setOffline(true);

  await pageA.getByLabel("Todo title").fill(offlineTitle);
  await pageA.getByRole("button", { name: "Add todo" }).click();

  const todoInA = pageA.locator("#todo-list li", { hasText: offlineTitle });
  const todoInB = pageB.locator("#todo-list li", { hasText: offlineTitle });

  await expect(todoInA).toHaveCount(1, { timeout: 10_000 });
  await expect(todoInB).toHaveCount(0);

  await contextA.setOffline(false);
  await pageA.reload();
  await pageB.reload();

  await expect(todoInB).toHaveCount(1, { timeout: 30_000 });

  await contextA.close();
  await contextB.close();
});
