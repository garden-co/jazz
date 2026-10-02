import { expect, test } from "@playwright/test";

test("persists edits, syncs between browsers, and saves offline", async ({ page, browser }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Create document" }).click();
  const editor = page.getByRole("textbox", { name: "Document" });
  await expect(editor).toHaveAttribute("contenteditable", "true");
  await editor.click();
  await editor.pressSequentially("Hello Jazz");

  const other = await browser.newContext();
  try {
    const reader = await other.newPage();
    await reader.goto(page.url());
    const remote = reader.getByRole("textbox", { name: "Document" });
    await expect(remote).toHaveText("Hello Jazz", { timeout: 30_000 });
    await page.reload();
    await expect(editor).toHaveText("Hello Jazz");
    await expect(editor).toHaveAttribute("contenteditable", "true");

    // Edit from the second replica only after it has received the first edit.
    await remote.fill("Hello from the other browser");
    await expect(editor).toHaveText("Hello from the other browser");

    const offline = page.getByRole("checkbox", { name: "Offline" });
    await offline.click();
    await expect(offline).toBeChecked();
    await expect(offline).toBeEnabled();
    await editor.fill("An offline edit");
    await expect(editor).toHaveText("An offline edit");
    // Give an accidental sync time to arrive before asserting isolation.
    await reader.waitForTimeout(500);
    await expect(remote).toHaveText("Hello from the other browser");
    await expect(page.getByRole("alert")).toHaveCount(0);
    await offline.click();
    await expect(offline).not.toBeChecked();
    await expect(remote).toHaveText("An offline edit", { timeout: 30_000 });
    await page.reload();
    await expect(editor).toHaveText("An offline edit");
  } finally {
    await other.close();
  }
});
