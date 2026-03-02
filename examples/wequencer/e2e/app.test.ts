import { test, expect } from "@playwright/test";

test.describe("app loading", () => {
  test("renders the nav with Wequencer title", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("nav h1")).toHaveText("Wequencer");
  });

  test("shows the Start Wequencing button before audio context is active", async ({ page }) => {
    await page.goto("/");
    const startButton = page.locator(".start-prompt button");
    await expect(startButton).toBeVisible({ timeout: 10_000 });
    await expect(startButton).toHaveText("Start Wequencing");
  });

  test("activating audio context reveals the sequencer grid", async ({ page }) => {
    await page.goto("/");
    const startButton = page.locator(".start-prompt button");
    await expect(startButton).toBeVisible({ timeout: 10_000 });
    await startButton.click();

    // Sequencer grid should appear with instrument rows
    const grid = page.locator(".sequencer .grid");
    await expect(grid).toBeVisible({ timeout: 10_000 });

    // Should have instrument name labels
    const instrumentNames = page.locator(".instrument-name");
    await expect(instrumentNames.first()).toBeVisible();
    const count = await instrumentNames.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });
});
