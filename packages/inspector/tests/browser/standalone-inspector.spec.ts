import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import {
  ADMIN_SECRET,
  APP_ID,
  SEEDED_TODO_COUNT,
  TEST_BRANCH,
  TEST_ENV,
  TEST_PORT,
} from "./test-constants.js";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
const STORAGE_KEY = "jazz-inspector-standalone-config";
const VISIBLE_ROW_COUNT = Math.min(SEEDED_TODO_COUNT, 10);
const RUNTIME_CONFIG_PATH = join(import.meta.dirname ?? __dirname, "runtime-config.json");

function storedConfig() {
  const { schemaHash } = readRuntimeConfig();
  return {
    serverUrl: SERVER_URL,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    env: TEST_ENV,
    branch: TEST_BRANCH,
    schemaHash,
  };
}

function readRuntimeConfig(): { schemaHash: string } {
  return JSON.parse(readFileSync(RUNTIME_CONFIG_PATH, "utf8")) as { schemaHash: string };
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

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 15000,
    });
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

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 15000,
    });
    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole("link", { name: "View todos data" })).toBeVisible({
      timeout: 15000,
    });
    await expect(page.getByRole("link", { name: "View todos schema" })).toBeVisible({
      timeout: 15000,
    });
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

    await expect(page.getByRole("button", { name: "Reset connection" })).toBeVisible({
      timeout: 15000,
    });
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

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 15000,
    });
    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole("link", { name: "View todos data" })).toBeVisible({
      timeout: 15000,
    });
    await expect(page.getByRole("link", { name: "View todos schema" })).toBeVisible({
      timeout: 15000,
    });
  });

  test("loads data explorer from stored config", async ({ page }) => {
    await page.getByRole("link", { name: "View todos data" }).click();

    await expect(page.getByText("3 columns")).toBeVisible();

    await expect(page.getByText(new RegExp(`${VISIBLE_ROW_COUNT} rows on page`))).toBeVisible({
      timeout: 10000,
    });
  });

  test("loads schema explorer", async ({ page }) => {
    await page.getByRole("link", { name: "View todos schema" }).click();

    await expect(page.getByText('"name": "title"')).toBeVisible();
    await expect(page.getByText('"type": "Text"')).toBeVisible();
    await expect(page.getByText('"name": "done"')).toBeVisible();
    await expect(page.getByText('"type": "Boolean"')).toBeVisible();
  });
});
