import { expect, test, type Locator, type Page } from "@playwright/test";

function articleCard(page: Page, title: string): Locator {
  return page.locator("article.article-card", {
    has: page.getByRole("heading", { name: title, exact: true }),
  });
}

async function openOnlyDraft(page: Page) {
  const drafts = page.locator(".drafts-section li a");
  await expect(drafts).toHaveCount(1);
  await drafts.click();
  await expect(page.getByLabel("Title", { exact: true })).toBeVisible();
}

async function saveDraft(page: Page) {
  const started = Date.now();
  await page.getByRole("button", { name: "Save draft" }).click();
  await expect(page.locator(".status")).toContainText("Saved", { timeout: 1_000 });
  const elapsed = Date.now() - started;
  expect(elapsed).toBeLessThan(1_000);
}

test.describe("Medium clone article flows", () => {
  test("drafts, publishing, editing, viewing, and discarding work end to end", async ({ page }) => {
    const suffix = Date.now();
    const title = `Branch Notes ${suffix}`;
    const updatedTitle = `Branch Notes Revised ${suffix}`;
    const discardedTitle = `Discarded Branch Notes ${suffix}`;

    await page.goto("/");
    await expect(page.getByRole("heading", { name: "Medium-ish" })).toBeVisible();

    await page.getByRole("button", { name: "New article" }).click();
    await expect(page.getByLabel("Title", { exact: true })).toBeVisible();
    await page.getByLabel("Title", { exact: true }).fill(title);
    await page.getByLabel("Subtitle", { exact: true }).fill("A private draft before publish");
    await page
      .getByLabel("Content", { exact: true })
      .fill("Alice writes privately before publishing.");
    await page.getByLabel("Labels", { exact: true }).fill("jazz, branches");
    await saveDraft(page);

    await page.getByRole("button", { name: "Back" }).click();
    await expect(page.locator(".drafts-section")).toContainText("Your drafts");
    await openOnlyDraft(page);
    await expect(page.getByLabel("Title", { exact: true })).toHaveValue(title);
    await expect(page.getByLabel("Content", { exact: true })).toHaveValue(
      "Alice writes privately before publishing.",
    );

    await page.reload();
    await expect(page.getByLabel("Title", { exact: true })).toHaveValue(title);

    await page.getByRole("button", { name: "Publish" }).click();
    await expect(articleCard(page, title)).toBeVisible();
    await expect(page.locator(".drafts-section")).toHaveCount(0);

    await articleCard(page, title).getByRole("heading", { name: title }).click();
    await expect(page.getByRole("heading", { name: title })).toBeVisible();
    await expect(page.getByText("Alice writes privately before publishing.")).toBeVisible();
    await page.getByRole("button", { name: "Back" }).click();

    await articleCard(page, title).getByRole("link", { name: "edit" }).click();
    await expect(page.getByLabel("Title", { exact: true })).toHaveValue(title);
    await page.getByLabel("Title", { exact: true }).fill(updatedTitle);
    await page.getByLabel("Subtitle", { exact: true }).fill("Updated subtitle");
    await page
      .getByLabel("Content", { exact: true })
      .fill("Bob edits a published article on a branch.");
    await page.getByLabel("Labels", { exact: true }).fill("jazz, revised");
    await saveDraft(page);
    await page.getByRole("button", { name: "Back" }).click();

    await expect(articleCard(page, title)).toBeVisible();
    await expect(articleCard(page, updatedTitle)).toHaveCount(0);
    await openOnlyDraft(page);
    await expect(page.getByLabel("Title", { exact: true })).toHaveValue(updatedTitle);

    await page.getByRole("button", { name: "Republish" }).click();
    await expect(articleCard(page, updatedTitle)).toBeVisible();
    await expect(articleCard(page, title)).toHaveCount(0);

    await articleCard(page, updatedTitle).getByRole("link", { name: "edit" }).click();
    await page.getByLabel("Title", { exact: true }).fill(discardedTitle);
    await saveDraft(page);
    page.once("dialog", (dialog) => dialog.accept());
    await page.getByRole("button", { name: "Discard" }).click();

    await expect(articleCard(page, updatedTitle)).toBeVisible();
    await expect(articleCard(page, discardedTitle)).toHaveCount(0);
    await expect(page.locator(".drafts-section")).toHaveCount(0);
  });
});
