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
  // Edge sync may fail after Local durability; either terminal status is safe to reload.
  await expect(page.getByRole("status")).toContainText("Saved locally", { timeout: TIMEOUT });
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

test("todo can be completed and deleted", async ({ page }) => {
  const runId = Date.now();
  const todo = `Finish me ${runId}`;

  await page.goto("/");
  await waitForApp(page);
  await addTodo(page, todo);

  const row = page.getByRole("listitem").filter({ hasText: todo });
  await row.getByRole("checkbox").check();
  await page.reload();
  await waitForApp(page);
  await expect(row.getByRole("checkbox")).toBeChecked({ timeout: TIMEOUT });

  await row.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByText(todo, { exact: true })).toHaveCount(0, { timeout: TIMEOUT });
  await page.reload();
  await waitForApp(page);
  await expect(page.getByText(todo, { exact: true })).toHaveCount(0, { timeout: TIMEOUT });
});
