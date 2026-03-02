import { test, expect } from "@playwright/test";

// Sync tests use separate browser contexts (separate users, separate storage).
// Context A creates a jam, then context B navigates to the same jam URL.
// Data syncs between them via the jazz sync server.

test.describe("multi-user sync", () => {
  test("beat placed by one user appears for the other", async ({ browser }) => {
    const contextA = await browser.newContext();
    const contextB = await browser.newContext();
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();

    try {
      // Context A loads and gets a jam (URL updates to /#/<JAM_ID>)
      await pageA.goto("/");
      await pageA.locator(".start-prompt button").click();
      await expect(pageA.locator(".sequencer .grid")).toBeVisible({ timeout: 15_000 });

      // Context B navigates to the same jam URL
      const jamUrl = pageA.url();
      await pageB.goto(jamUrl);
      await pageB.locator(".start-prompt button").click();
      await expect(pageB.locator(".sequencer .grid")).toBeVisible({ timeout: 15_000 });

      // Place a beat in context A
      const cellA = pageA.locator(".beat-cell").nth(1);
      await cellA.click();
      await expect(cellA).toHaveClass(/active/);

      // Should appear in context B
      const cellB = pageB.locator(".beat-cell").nth(1);
      await expect(cellB).toHaveClass(/active/, { timeout: 30_000 });
    } finally {
      await contextA.close();
      await contextB.close();
    }
  });

  test("beat removed by one user disappears for the other", async ({ browser }) => {
    const contextA = await browser.newContext();
    const contextB = await browser.newContext();
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();

    try {
      await pageA.goto("/");
      await pageA.locator(".start-prompt button").click();
      await expect(pageA.locator(".sequencer .grid")).toBeVisible({ timeout: 15_000 });

      const jamUrl = pageA.url();
      await pageB.goto(jamUrl);
      await pageB.locator(".start-prompt button").click();
      await expect(pageB.locator(".sequencer .grid")).toBeVisible({ timeout: 15_000 });

      // Place a beat in context A
      const cellA = pageA.locator(".beat-cell").nth(2);
      await cellA.click();
      await expect(cellA).toHaveClass(/active/);

      // Wait for it to appear in context B
      const cellB = pageB.locator(".beat-cell").nth(2);
      await expect(cellB).toHaveClass(/active/, { timeout: 30_000 });

      // Remove the beat in context A
      await cellA.click();
      await expect(cellA).not.toHaveClass(/active/);

      // Should disappear from context B
      await expect(cellB).not.toHaveClass(/active/, { timeout: 30_000 });
    } finally {
      await contextA.close();
      await contextB.close();
    }
  });

  test("both users appear in the participants list", async ({ browser }) => {
    const contextA = await browser.newContext();
    const contextB = await browser.newContext();
    const pageA = await contextA.newPage();
    const pageB = await contextB.newPage();

    try {
      await pageA.goto("/");
      await pageA.locator(".start-prompt button").click();
      await expect(pageA.locator(".participants")).toBeVisible({ timeout: 15_000 });

      const jamUrl = pageA.url();
      await pageB.goto(jamUrl);
      await pageB.locator(".start-prompt button").click();
      await expect(pageB.locator(".participants")).toBeVisible({ timeout: 15_000 });

      // Both should eventually see 2 participants
      await expect(pageA.locator(".participant")).toHaveCount(2, { timeout: 30_000 });
      await expect(pageB.locator(".participant")).toHaveCount(2, { timeout: 30_000 });
    } finally {
      await contextA.close();
      await contextB.close();
    }
  });
});
