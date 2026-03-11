import { expect, test } from "@playwright/test";
import { ADMIN_SECRET, APP_ID, TEST_BRANCH, TEST_ENV, TEST_PORT } from "./test-constants.js";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
const SCHEMA_HASH = "a01f5c72ec47a3f7d91a6862b4c5779d194f586ff8b432d92aecde954c306e9c";
const STORAGE_KEY = "jazz-inspector-standalone-config";

function storedConfig() {
  return {
    serverUrl: SERVER_URL,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    env: TEST_ENV,
    branch: TEST_BRANCH,
    schemaHash: SCHEMA_HASH,
  };
}

test.describe("connection page", () => {
  test("prefills connection form from hash fragment", async ({ page }) => {
    const fragment = new URLSearchParams({
      url: SERVER_URL,
      appId: APP_ID,
      adminSecret: ADMIN_SECRET,
    }).toString();
    await page.goto(`/#${fragment}`);

    await expect(page.getByLabel("Server URL")).toHaveValue(SERVER_URL);
    await expect(page.getByLabel("App ID")).toHaveValue(APP_ID);
    await expect(page.getByLabel("Admin secret")).toHaveValue(ADMIN_SECRET);
  });

  test("connects to server, shows schema selection and loads data explorer", async ({ page }) => {
    await page.goto("/");
    await page.getByLabel("Server URL").fill(SERVER_URL);
    await page.getByLabel("App ID").fill(APP_ID);
    await page.getByLabel("Admin secret").fill(ADMIN_SECRET);
    await page.getByRole("button", { name: "Connect" }).click();

    await expect(page.getByRole("heading", { name: "Select schema" })).toBeVisible();
    await expect(page.getByRole("option")).toHaveCount(2);

    await page.getByLabel("Schema hash").selectOption({ index: 1 });

    await page.getByRole("button", { name: "Use schema" }).click();

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible();
  });

  test("loads data explorer from stored config", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(
      ({ key, config }) => {
        localStorage.setItem(key, JSON.stringify(config));
      },
      { key: STORAGE_KEY, config: storedConfig() },
    );
    await page.reload();

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible();
    await expect(page.getByRole("link", { name: "View todos data" })).toBeVisible();
    await expect(page.getByRole("link", { name: "View todos schema" })).toBeVisible();
  });

  test("reset connection returns to onboarding", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(
      ({ key, config }) => {
        localStorage.setItem(key, JSON.stringify(config));
      },
      { key: STORAGE_KEY, config: storedConfig() },
    );
    await page.reload();

    await expect(page.getByRole("button", { name: "Reset connection" })).toBeVisible();
    await page.getByRole("button", { name: "Reset connection" }).click();
    await expect(page.getByRole("heading", { name: "Connect to Jazz server" })).toBeVisible();
  });
});

test.describe("data explorer page", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.evaluate(
      ({ key, config }) => {
        localStorage.setItem(key, JSON.stringify(config));
      },
      { key: STORAGE_KEY, config: storedConfig() },
    );

    await page.reload();

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible();
    await expect(page.getByRole("link", { name: "View todos data" })).toBeVisible();
    await expect(page.getByRole("link", { name: "View todos schema" })).toBeVisible();
  });

  test("loads data explorer from stored config", async ({ page }) => {
    await page.getByRole("link", { name: "View todos data" }).click();

    await expect(page.getByText("3 columns")).toBeVisible();

    await expect(page.getByText("2 rows")).toBeVisible({ timeout: 10000 });
  });

  test("loads schema explorer", async ({ page }) => {
    await page.getByRole("link", { name: "View todos schema" }).click();

    await expect(page.getByText("title TEXT NOT NULL")).toBeVisible();
    await expect(page.getByText("done BOOLEAN NOT NULL")).toBeVisible();
  });
});
