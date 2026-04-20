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

test("recovery phrase round-trips the local-first identity", async ({ page }) => {
  const runId = Date.now();
  const todo = `Backup todo ${runId}`;

  await page.goto("/");
  await waitForApp(page);
  await addTodo(page, todo);

  // Reveal the phrase.
  await page.getByText("Back up or restore your local-only account").click();
  await page.getByRole("button", { name: "Show recovery phrase" }).click();
  const phraseTextarea = page.getByLabel("Recovery phrase", { exact: true });
  await expect(phraseTextarea).not.toHaveValue("", { timeout: TIMEOUT });
  const phrase = await phraseTextarea.inputValue();
  expect(phrase.trim().split(/\s+/).length).toBe(24);

  // Clear local storage → a fresh anonymous identity is generated, todo vanishes.
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await waitForApp(page);
  await expect(page.getByText(todo)).toHaveCount(0, { timeout: TIMEOUT });

  // Restore the phrase.
  await page.getByText("Back up or restore your local-only account").click();
  await page.getByLabel("Restore from recovery phrase").fill(phrase);
  await page.getByRole("button", { name: "Restore", exact: true }).click();

  // Page reloads; the original todo should reappear.
  await waitForApp(page);
  await expect(page.getByText(todo)).toBeVisible({ timeout: TIMEOUT });
});
