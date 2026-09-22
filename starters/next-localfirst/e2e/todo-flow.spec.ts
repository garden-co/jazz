import { expect, test, type Page } from "@playwright/test";

const TODO_INPUT_LABEL = "New todo";
const TIMEOUT = 20_000;

async function waitForApp(page: Page) {
  await expect(page.getByLabel(TODO_INPUT_LABEL)).toBeVisible({
    timeout: TIMEOUT,
  });
}

async function addTodo(page: Page, title: string) {
  await page.getByLabel(TODO_INPUT_LABEL).fill(title);
  await page.getByRole("button", { name: "Add" }).click();
  await expect(page.getByText(title, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
  await expect(page.getByRole("status")).toHaveText("Saved locally", { timeout: TIMEOUT });
}

test("todo persists across reload in pure local-first mode", async ({ page }) => {
  const runId = Date.now();
  const todo = `Persistent todo ${runId}`;

  await page.goto("/");
  await waitForApp(page);

  await addTodo(page, todo);

  await page.reload();
  await waitForApp(page);
  await expect(page.getByText(todo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
});

test("development inspector displays the app's local todos", async ({ page }) => {
  test.skip(process.env.JAZZ_E2E_PROD === "1", "The inspector is development-only");
  const todo = `Inspector todo ${Date.now()}`;

  await page.goto("/");
  await waitForApp(page);
  await addTodo(page, todo);

  const toggle = page.getByRole("button", { name: "Open Jazz inspector" });
  await expect(toggle).toBeVisible({ timeout: TIMEOUT });
  await toggle.click();

  const inspector = page.frameLocator('iframe[title="Jazz inspector"]');
  await expect(inspector.getByRole("link", { name: "View todos data" })).toBeVisible({
    timeout: TIMEOUT,
  });
  await inspector.getByRole("link", { name: "View todos data" }).click();
  await expect(inspector.getByRole("gridcell", { name: todo, exact: true })).toBeVisible({
    timeout: TIMEOUT,
  });
});
