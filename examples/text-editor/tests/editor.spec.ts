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

test("merges offline edits from separate sessions and keeps them after reload", async ({
  page,
  browser,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Create document" }).click();
  const editor = page.getByRole("textbox", { name: "Document" });
  await expect(editor).toHaveAttribute("contenteditable", "true");
  await editor.fill("Start");

  const other = await browser.newContext();
  const fresh = await browser.newContext();
  try {
    const remotePage = await other.newPage();
    await remotePage.goto(page.url());
    const remote = remotePage.getByRole("textbox", { name: "Document" });
    await expect(remote).toHaveText("Start", { timeout: 30_000 });

    // This tab shares the first account and worker, but must own a different log.
    const tab = await page.context().newPage();
    await tab.goto(page.url());
    const sibling = tab.getByRole("textbox", { name: "Document" });
    await expect(sibling).toHaveText("Start");

    const offline = page.getByRole("checkbox", { name: "Offline" });
    await offline.click();
    await expect(offline).toBeChecked();
    await expect(offline).toBeEnabled();

    await editor.press("ControlOrMeta+End");
    await editor.pressSequentially(" Alice");
    await expect(sibling).toHaveText("Start Alice");
    await sibling.press("ControlOrMeta+End");
    await sibling.pressSequentially(" Tab");
    await expect(editor).toHaveText("Start Alice Tab");

    await remote.press("ControlOrMeta+End");
    await remote.pressSequentially(" Bob");
    await expect(remote).toHaveText("Start Bob");
    await expect(editor).toHaveText("Start Alice Tab");

    await offline.click();
    await expect(offline).not.toBeChecked();
    await expect(editor).toContainText("Bob", { timeout: 30_000 });
    const merged = await editor.innerText();
    for (const word of ["Start", "Alice", "Tab", "Bob"]) {
      expect(merged.split(word)).toHaveLength(2);
    }
    await expect(remote).toHaveText(merged);
    await expect(sibling).toHaveText(merged);

    await page.reload();
    await remotePage.reload();
    await expect(editor).toHaveText(merged);
    await expect(remote).toHaveText(merged);
    await expect(editor).toHaveAttribute("contenteditable", "true");
    await editor.press("ControlOrMeta+End");
    await editor.pressSequentially(" Reload");
    await expect(remote).toHaveText(`${merged} Reload`);

    const newcomer = await fresh.newPage();
    await newcomer.goto(page.url());
    await expect(newcomer.getByRole("textbox", { name: "Document" })).toHaveText(
      `${merged} Reload`,
      { timeout: 30_000 },
    );
    await expect(page.getByRole("alert")).toHaveCount(0);
    await expect(remotePage.getByRole("alert")).toHaveCount(0);
  } finally {
    await other.close();
    await fresh.close();
  }
});
