import { expect, test, type Page } from "@playwright/test";

test("creates & searches journal entries", async ({ page }) => {
  await page.goto("/");

  // Assert empty state
  await expect(page.getByText("Start the personal journal.")).toBeVisible();

  // Create entries
  await createEntry(page, "A");
  await expect(page.locator(".journal-entry-card")).toHaveText("A", {
    timeout: 30_000,
  });

  await createEntry(page, "B");
  await createEntry(page, "C");
  await createEntry(page, "D");
  await createEntry(page, "E");
  await createEntry(page, "F");
  await createEntry(page, "G");
  await createEntry(page, "H");
  await createEntry(page, "I");

  // Shows all entries
  await expect(page.locator(".journal-entry-card")).toHaveCount(9);

  // Search for "G"
  const searchbox = page.getByRole("searchbox");
  await searchbox.fill("G");
  await searchbox.press("Enter");

  // Shows 5 results
  await expect(page.locator(".journal-entry-card")).toHaveCount(5);
  const entryCards = page.locator(".journal-entry-card");
  await expect(entryCards).toHaveCount(5);

  // Assert order of results
  const entryCardsCount = await entryCards.count();
  const expectations: [string, string][] = [
    ["G", "1"],
    ["B", "0.52"],
    ["E", "0.49"],
    ["C", "0.49"],
    ["D", "0.48"],
  ];

  for (let i = 0; i < entryCardsCount; i++) {
    const entryContent = entryCards.nth(i).locator("div").nth(0);
    const entrySimilarity = entryCards.nth(i).locator("span span").nth(0);

    const [expectedText, expectedSimilarity] = expectations[i];

    await expect(entryContent).toHaveText(expectedText);
    await expect(entrySimilarity).toHaveText(expectedSimilarity);
  }
});

async function createEntry(page: Page, text: string) {
  const createEntryButton = page.getByRole("button", { name: "+ New entry" });

  createEntryButton.click();
  const dialog = await page.waitForEvent("dialog");

  expect(dialog.type()).toBe("prompt");
  expect(dialog.message().toLowerCase()).toContain("on your mind");

  await dialog.accept(text);
}
