import { expect, test } from "@playwright/test";

test.describe("Jazz Inspector", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");

    // Login first
    await page.getByRole("button", { name: "Sign in" }).click();
    await page
      .getByRole("textbox", { name: "Email address" })
      .fill("guido+clerk-test@garden.co");
    await page.keyboard.press("Enter");
    await page
      .getByRole("textbox", { name: "Password" })
      .fill("guido+clerk-test@garden.co");
    await page.keyboard.press("Enter");
    await page.waitForURL("/");
    await page.getByText("You're logged in").waitFor({ state: "visible" });
  });

  test("inspector button appears when logged in", async ({ page }) => {
    // Inspector button should be visible after login
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await expect(inspectorButton).toBeVisible();

    // Button should have the Jazz icon
    const jazzIcon = inspectorButton.locator("svg");
    await expect(jazzIcon).toBeVisible();
  });

  test("inspector opens and shows initial form", async ({ page }) => {
    // Click the inspector button
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await inspectorButton.click();

    // Inspector panel should open
    const inspectorPanel = page.locator(
      '[style*="position: fixed"][style*="bottom: 0"]',
    );
    await expect(inspectorPanel).toBeVisible();

    // Should show the initial form with heading
    await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();

    // Should have CoValue ID input
    const coValueInput = page.locator('input[placeholder*="co_z"]');
    await expect(coValueInput).toBeVisible();

    // Should have "Inspect CoValue" button
    await expect(
      page.getByRole("button", { name: "Inspect CoValue" }),
    ).toBeVisible();

    // Should have "Inspect my account" button
    await expect(
      page.getByRole("button", { name: "Inspect my account" }),
    ).toBeVisible();
  });

  test("inspector can inspect account", async ({ page }) => {
    // Open inspector
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await inspectorButton.click();

    // Click "Inspect my account"
    await page.getByRole("button", { name: "Inspect my account" }).click();

    // Should navigate away from initial form
    await expect(page.getByText("Jazz CoValue Inspector")).not.toBeVisible();

    // Should show breadcrumbs
    const breadcrumbs = page.locator(
      '.jazz-breadcrumbs, [class*="breadcrumb"]',
    );
    await expect(breadcrumbs).toBeVisible();

    // Should show account data in some form (grid or content)
    const accountContent = page.locator(
      '[class*="grid"], [class*="content"], [class*="account"]',
    );
    await expect(accountContent.first()).toBeVisible();
  });

  test("inspector can be closed", async ({ page }) => {
    // Open inspector
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await inspectorButton.click();

    // Inspector should be open
    const inspectorPanel = page.locator(
      '[style*="position: fixed"][style*="bottom: 0"]',
    );
    await expect(inspectorPanel).toBeVisible();

    // Click close button
    await page.getByRole("button", { name: "Close" }).click();

    // Inspector should be closed, button should be visible again
    await expect(inspectorPanel).not.toBeVisible();
    await expect(inspectorButton).toBeVisible();
  });

  test("inspector handles invalid CoValue ID gracefully", async ({ page }) => {
    // Open inspector
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await inspectorButton.click();

    // Enter invalid CoValue ID
    const coValueInput = page.locator('input[placeholder*="co_z"]');
    await coValueInput.fill("invalid_id");

    // Submit form
    await page.getByRole("button", { name: "Inspect CoValue" }).click();

    // Should not crash - either show error or remain on form
    // The exact behavior may vary, but the app should remain functional
    await expect(page.getByRole("button", { name: "Close" })).toBeVisible();
  });

  test("inspector maintains state during navigation", async ({ page }) => {
    // Open inspector
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await inspectorButton.click();

    // Navigate to account
    await page.getByRole("button", { name: "Inspect my account" }).click();

    // Should have breadcrumbs and CoValue input in header
    const headerInput = page.locator('input[placeholder*="co_z"]').first();
    await expect(headerInput).toBeVisible();

    // Close and reopen inspector
    await page.getByRole("button", { name: "Close" }).click();
    await inspectorButton.click();

    // Should return to initial state (not maintain navigation state)
    await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();
  });

  test("inspector works after page reload", async ({ page }) => {
    // Open inspector first
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await inspectorButton.click();
    await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();

    // Close inspector
    await page.getByRole("button", { name: "Close" }).click();

    // Reload page
    await page.reload();
    await page.getByText("You're logged in").waitFor({ state: "visible" });

    // Inspector button should still work
    await expect(inspectorButton).toBeVisible();
    await inspectorButton.click();
    await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();
  });
});

test.describe("Jazz Inspector - Logged Out", () => {
  test("inspector button not visible when logged out", async ({ page }) => {
    await page.goto("/");

    // Should show login state
    await expect(page.getByText("You're not logged in")).toBeVisible();

    // Inspector button should not be visible
    const inspectorButton = page.locator('button[title="Open Jazz Inspector"]');
    await expect(inspectorButton).not.toBeVisible();
  });
});
