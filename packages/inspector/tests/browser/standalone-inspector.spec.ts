import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { ADMIN_SECRET, APP_ID, TEST_BRANCH, TEST_ENV, TEST_PORT } from "./test-constants.js";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
const STORAGE_KEY = "jazz-inspector-standalone-config";
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

async function storeStandaloneConfig(page: Page) {
  await page.evaluate(
    ({ key, config }) => {
      localStorage.setItem(key, JSON.stringify(config));
    },
    { key: STORAGE_KEY, config: storedConfig() },
  );
}

async function expectTodosTableLoaded(page: Page) {
  await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible({ timeout: 15_000 });
  await expect(page.getByRole("link", { name: "Schema" })).toBeVisible({ timeout: 5_000 });
}

async function openTodosTable(page: Page) {
  await page.goto("/");
  await storeStandaloneConfig(page);
  await page.reload();

  await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible({
    timeout: 5_000,
  });

  const tableLink = page.getByRole("link", { name: "View todos data" });
  await expect(tableLink).toBeVisible({ timeout: 5_000 });
  await tableLink.click();

  await expectTodosTableLoaded(page);
}

function rowByTitle(page: Page, title: string) {
  return page.locator('[role="row"]').filter({
    has: page.getByRole("gridcell", { name: title, exact: true }),
  });
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
    await storeStandaloneConfig(page);
    await page.reload();

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible({
      timeout: 15000,
    });
    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole("link", { name: "View todos data" })).toBeVisible({
      timeout: 15000,
    });
    await expect(page.getByRole("heading", { name: "Select a table" })).toBeVisible({
      timeout: 15000,
    });
  });

  test("reset connection returns to onboarding", async ({ page }) => {
    await page.goto("/");
    await storeStandaloneConfig(page);
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
    await openTodosTable(page);
  });

  test("loads data explorer from stored config", async ({ page }) => {
    await page.getByRole("link", { name: "View todos data" }).click();

    await expect(page.getByText("First seeded todo")).toBeVisible();
  });

  test("loads schema explorer", async ({ page }) => {
    await page.getByRole("link", { name: "Schema" }).click();

    await expect(page.getByText('"name": "title"')).toBeVisible();
    await expect(page.getByText('"type": "Text"')).toBeVisible();
    await expect(page.getByText('"name": "done"')).toBeVisible();
    await expect(page.getByText('"type": "Boolean"')).toBeVisible();
  });

  test("discards queued inline text edits without persisting them", async ({ page }) => {
    const originalTitle = "Seeded todo 000003";
    const updatedTitle = `Discarded inline edit ${Date.now()}`;

    await page.getByRole("gridcell", { name: originalTitle, exact: true }).dblclick();

    const editor = page.getByLabel("Edit title");
    await editor.fill(updatedTitle);
    await editor.press("Enter");

    const queuedBanner = page.getByRole("status");
    await expect(queuedBanner).toContainText("Queued");
    await expect(queuedBanner).toContainText("1 edit across 1 row");
    await expect(page.getByRole("gridcell", { name: updatedTitle, exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Discard" }).click();

    await expect(page.getByRole("button", { name: "Discard" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Save changes" })).toHaveCount(0);
    await expect(page.getByRole("gridcell", { name: originalTitle, exact: true })).toBeVisible();
    await expect(page.getByRole("gridcell", { name: updatedTitle, exact: true })).toHaveCount(0);

    await openTodosTable(page);
    await expect(page.getByRole("gridcell", { name: originalTitle, exact: true })).toBeVisible();
    await expect(page.getByRole("gridcell", { name: updatedTitle, exact: true })).toHaveCount(0);
  });

  test("saves inline text cell edits and keeps them after refresh", async ({ page }) => {
    const originalTitle = "First seeded todo";
    const updatedTitle = `Edited ${Date.now()}`;

    await page.getByRole("gridcell", { name: originalTitle, exact: true }).dblclick();

    const editor = page.getByLabel("Edit title");
    await editor.fill(updatedTitle);
    await editor.press("Enter");

    const queuedBanner = page.getByRole("status");
    await expect(queuedBanner).toContainText("Queued");
    await expect(queuedBanner).toContainText("1 edit across 1 row");

    await page.getByRole("button", { name: "Save changes" }).click();
    await expect(page.getByRole("button", { name: "Save changes" })).toHaveCount(0);

    await openTodosTable(page);
    await expect(page.getByRole("gridcell", { name: updatedTitle, exact: true })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByRole("gridcell", { name: originalTitle, exact: true })).toHaveCount(0);
  });

  test("saves inline boolean cell edits and keeps them after refresh", async ({ page }) => {
    const title = "Second seeded todo";
    const targetRow = rowByTitle(page, title);
    const toggle = targetRow.getByRole("checkbox");

    const initialChecked = await toggle.isChecked();
    await toggle.click();

    const queuedBanner = page.getByRole("status");
    await expect(queuedBanner).toContainText("Queued");
    await expect(queuedBanner).toContainText("1 edit across 1 row");

    await page.getByRole("button", { name: "Save changes" }).click();
    await expect(page.getByRole("button", { name: "Save changes" })).toHaveCount(0);

    await openTodosTable(page);
    await expect(rowByTitle(page, title).getByRole("checkbox")).toHaveJSProperty(
      "checked",
      !initialChecked,
      {
        timeout: 15_000,
      },
    );
  });

  test("filters rows to done=true and shows only checked boolean cells", async ({ page }) => {
    const visibleCheckboxesBeforeFilter = page.getByRole("checkbox", { name: /Toggle done for/ });
    const visibleCheckboxCountBeforeFilter = await visibleCheckboxesBeforeFilter.count();
    expect(visibleCheckboxCountBeforeFilter).toBeGreaterThan(0);

    let uncheckedBeforeFilter = 0;
    for (let index = 0; index < visibleCheckboxCountBeforeFilter; index += 1) {
      if (!(await visibleCheckboxesBeforeFilter.nth(index).isChecked())) {
        uncheckedBeforeFilter += 1;
      }
    }
    expect(uncheckedBeforeFilter).toBeGreaterThan(0);

    await page.getByRole("button", { name: "Filter" }).click();

    const dialog = page.getByRole("dialog", { name: "Filter rows" });
    await expect(dialog).toBeVisible();

    await dialog.getByLabel("Column").selectOption("done");
    await dialog.getByLabel("Value").fill("true");
    await dialog.getByRole("button", { name: "Add where clause" }).click();
    await dialog.getByRole("button", { name: "Close" }).click();

    const filterButton = page.getByRole("button", { name: "Filter (1)" });
    await expect(dialog).not.toBeVisible();
    await expect(filterButton).toBeVisible();

    const checkboxes = page.getByRole("checkbox", { name: /Toggle done for/ });
    const checkboxCount = await checkboxes.count();
    expect(checkboxCount).toBeGreaterThan(0);

    for (let index = 0; index < checkboxCount; index += 1) {
      await expect(checkboxes.nth(index)).toBeChecked();
    }
  });
});
