import { expect, test, type Page } from "@playwright/test";
import { ADMIN_SECRET, APP_ID, TEST_BRANCH, TEST_ENV, TEST_PORT } from "./test-constants.js";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
const SCHEMA_HASH = process.env.PUBLISHED_SCHEMA_HASH;
const STORAGE_KEY = "jazz-inspector-standalone-config";

function storedConfig() {
  return {
    version: 2,
    activeConnectionId: "test-connection",
    connections: [
      {
        id: "test-connection",
        name: "Browser test",
        serverUrl: SERVER_URL,
        appId: APP_ID,
        adminSecret: ADMIN_SECRET,
        env: TEST_ENV,
        branch: TEST_BRANCH,
        schemaHash: SCHEMA_HASH,
      },
    ],
  };
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
  await expect(page.getByRole("checkbox", { name: /Toggle done for/ }).first()).toBeVisible({
    timeout: 15_000,
  });
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
  await page.getByRole("columnheader", { name: "title" }).click();

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
      serverUrl: SERVER_URL,
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
    await storeStandaloneConfig(page);
    await page.reload();

    await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Tables" })).toBeVisible();
    await expect(page.getByRole("link", { name: "View todos data" })).toBeVisible();
  });

  test("connection manager opens a prefilled edit screen", async ({ page }) => {
    await page.goto("/");
    await storeStandaloneConfig(page);
    await page.reload();

    await expect(page.getByRole("button", { name: "Connections" })).toBeVisible();

    await page.getByRole("button", { name: "Connections" }).click();

    await expect(page.getByRole("heading", { name: "Connections" })).toBeVisible();
    await expect(page.getByText("Browser test")).toBeVisible();

    await page.getByRole("button", { name: "Edit Browser test" }).click();

    await expect(page.getByRole("heading", { name: "Edit connection" })).toBeVisible();
    await expect(page.getByLabel("Name")).toHaveValue("Browser test");
    await expect(page.getByLabel("Server URL")).toHaveValue(SERVER_URL);
    await expect(page.getByLabel("App ID")).toHaveValue(APP_ID);
    await expect(page.getByLabel("Admin secret")).toHaveValue(ADMIN_SECRET);
    await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible();
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
    await expect(page.getByRole("heading", { name: "todos permissions" })).toBeVisible();
    await expect(page.getByText('"insert"')).toBeVisible();
    await expect(page.getByText('"update"')).toBeVisible();
    await expect(page.getByText('"delete"')).toBeVisible();
  });

  test("opens inline text editor in selected row", async ({ page }) => {
    const title = "First seeded todo";
    const titleCell = page.getByRole("gridcell", { name: title, exact: true });

    await titleCell.dblclick();

    await expect(page.getByLabel("Edit title")).toBeVisible();
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
    const checkboxes = page.getByRole("checkbox", { name: /Toggle done for/ });
    // Sorting and filtering replace the grid while the new query loads. Read
    // one DOM snapshot per attempt so counts and states describe the same rows.
    const readCheckedStates = () =>
      checkboxes.evaluateAll((elements) =>
        elements.map((element) => (element as HTMLInputElement).checked),
      );
    await expect(async () => {
      const checkedStates = await readCheckedStates();
      expect(checkedStates.length).toBeGreaterThan(0);
      expect(checkedStates).toContain(false);
    }).toPass({ timeout: 15_000 });

    const filters = page.getByRole("region", { name: "Filter rows" });
    await filters.getByLabel("Column", { exact: true }).selectOption("done");
    await filters.getByLabel("Value", { exact: true }).fill("true");
    await filters.getByRole("button", { name: "Add where clause" }).click();
    await expect(filters.getByText("done eq true", { exact: true })).toBeVisible();

    await expect(async () => {
      const checkedStates = await readCheckedStates();
      expect(checkedStates.length).toBeGreaterThan(0);
      expect(checkedStates).not.toContain(false);
    }).toPass({ timeout: 15_000 });
  });
});
