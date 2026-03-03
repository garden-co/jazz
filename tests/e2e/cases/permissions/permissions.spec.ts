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

const BASE_URL = "http://127.0.0.1:5173/cases/permissions/index.html";
const APP_ID = "00000000-0000-0000-0000-000000000001";
const SYNC_PORT = 1625;
const ADMIN_SECRET = "permissions-e2e-admin-secret";

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

test("everyone can read todos", async ({ browser }) => {
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();

  await pageA.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "permissions-client-a",
      serverUrl: server.url,
    }),
  );

  await pageB.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "permissions-client-b",
      serverUrl: server.url,
    }),
  );

  await expect(pageA.getByRole("heading", { name: "Permissions App" })).toBeVisible();
  await expect(pageB.getByRole("heading", { name: "Permissions App" })).toBeVisible();

  const firstTaskWithOwner = "first";
  await pageA.getByLabel("Todo title").fill(firstTaskWithOwner);
  await pageA.getByRole("button", { name: "Add todo" }).click();
  await expect(pageB.locator("#todo-list li", { hasText: firstTaskWithOwner })).toHaveCount(1, {
    timeout: 20_000,
  });

  await contextA.close();
  await contextB.close();
});

test("only owner can delete todos", async ({ browser }) => {
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();

  await pageA.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "permissions-client-a",
      serverUrl: server.url,
    }),
  );

  await pageB.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "permissions-client-b",
      serverUrl: server.url,
    }),
  );

  await expect(pageA.getByRole("heading", { name: "Permissions App" })).toBeVisible();
  await expect(pageB.getByRole("heading", { name: "Permissions App" })).toBeVisible();

  // A writes task
  const task = "first";
  await pageA.getByLabel("Todo title").fill(task);
  await pageA.getByRole("button", { name: "Add todo" }).click();
  await expect(pageB.locator("#todo-list li", { hasText: task })).toHaveCount(1, {
    timeout: 20_000,
  });

  // B tries to delete the task
  await pageB.getByRole("button", { name: "Delete todo" }).click();

  // wait for sync
  await new Promise((resolve) => setTimeout(resolve, 1000));

  // A still has the task
  await expect(pageA.locator("#todo-list li", { hasText: task })).toHaveCount(1);

  await contextA.close();
  await contextB.close();
});

test("only owner can update todos", async ({ browser }) => {
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();

  await pageA.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "permissions-client-a",
      serverUrl: server.url,
    }),
  );

  await pageB.goto(
    buildClientUrl({
      appId: APP_ID,
      token: "permissions-client-b",
      serverUrl: server.url,
    }),
  );

  await expect(pageA.getByRole("heading", { name: "Permissions App" })).toBeVisible();
  await expect(pageB.getByRole("heading", { name: "Permissions App" })).toBeVisible();

  // A writes task
  const task = "first";
  await pageA.getByLabel("Todo title").fill(task);
  await pageA.getByRole("button", { name: "Add todo" }).click();
  await expect(pageB.locator("#todo-list li", { hasText: task })).toHaveCount(1, {
    timeout: 20_000,
  });

  // B tries to delete the task
  await pageB.getByRole("checkbox").click();

  // wait for sync
  await new Promise((resolve) => setTimeout(resolve, 1000));

  // A has the task marked as done
  await expect(pageA.locator("#todo-list li", { hasText: task })).toHaveCount(1);
  await expect(pageA.locator("#todo-list li", { hasText: `${task} (done)` })).toHaveCount(0);

  await contextA.close();
  await contextB.close();
});
