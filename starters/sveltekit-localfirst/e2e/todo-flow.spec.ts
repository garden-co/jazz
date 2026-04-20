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
  await expect(page.getByText(title)).toBeVisible({ timeout: TIMEOUT });
}

test("todo persists across reload in pure local-first mode", async ({ page }) => {
  const runId = Date.now();
  const todo = `Persistent todo ${runId}`;

  await page.goto("/");
  await waitForApp(page);

  await addTodo(page, todo);

  await page.reload();
  await waitForApp(page);
  await expect(page.getByText(todo)).toBeVisible({ timeout: TIMEOUT });
});
