import { test, expect } from "@playwright/test";

test.describe("beat grid interaction", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const startButton = page.locator(".start-prompt button");
    await expect(startButton).toBeVisible({ timeout: 10_000 });
    await startButton.click();
    await expect(page.locator(".sequencer .grid")).toBeVisible({ timeout: 10_000 });
    // Wait for all 7 instruments to finish seeding (each fetches an MP3 from the dev server)
    await expect(page.locator(".instrument-name")).toHaveCount(7, { timeout: 60_000 });
  });

  test("renders all default instruments", async ({ page }) => {
    const names = page.locator(".instrument-name");
    await expect(names).toHaveCount(7);

    const expectedNames = ["Kick", "Snare", "Hi-hat", "Piano 1", "Piano 2", "Guitar 1", "Guitar 2"];
    for (let i = 0; i < expectedNames.length; i++) {
      await expect(names.nth(i)).toHaveText(expectedNames[i]);
    }
  });

  test("renders 16 beat cells per instrument", async ({ page }) => {
    const firstInstrumentName = page.locator(".instrument-name").first();
    await expect(firstInstrumentName).toBeVisible();

    // Total cells = 7 instruments x 16 beats = 112
    const allCells = page.locator(".beat-cell");
    await expect(allCells).toHaveCount(7 * 16);
  });

  test("marks every 4th beat as a downbeat", async ({ page }) => {
    const downbeats = page.locator(".beat-cell.downbeat");
    // 7 instruments x 4 downbeats (indices 0, 4, 8, 12) = 28
    await expect(downbeats).toHaveCount(7 * 4);
  });

  test("clicking an empty cell places a beat", async ({ page }) => {
    const firstCell = page.locator(".beat-cell").first();

    // Should not have a coloured background initially
    const bgBefore = await firstCell.evaluate((el) => getComputedStyle(el).backgroundColor);

    await firstCell.click();

    // After clicking, the cell should have an active class and a coloured background
    await expect(firstCell).toHaveClass(/active/);
    const bgAfter = await firstCell.evaluate((el) => getComputedStyle(el).backgroundColor);
    expect(bgAfter).not.toBe(bgBefore);
  });

  test("clicking a filled cell removes the beat", async ({ page }) => {
    const firstCell = page.locator(".beat-cell").first();

    // Place a beat
    await firstCell.click();
    await expect(firstCell).toHaveClass(/active/);

    // Remove the beat
    await firstCell.click();
    await expect(firstCell).not.toHaveClass(/active/);
  });

  test("multiple beats can be placed across instruments", async ({ page }) => {
    const cells = page.locator(".beat-cell");

    // Place beats on first cell of first 3 instruments (indices 0, 16, 32)
    await cells.nth(0).click();
    await cells.nth(16).click();
    await cells.nth(32).click();

    await expect(cells.nth(0)).toHaveClass(/active/);
    await expect(cells.nth(16)).toHaveClass(/active/);
    await expect(cells.nth(32)).toHaveClass(/active/);

    // Other cells should remain inactive
    await expect(cells.nth(1)).not.toHaveClass(/active/);
  });
});
