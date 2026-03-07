import { test, expect } from "@playwright/test";

test.describe("instrument management", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const startButton = page.locator(".start-prompt button");
    await expect(startButton).toBeVisible({ timeout: 10_000 });
    await startButton.click();
    await expect(page.locator(".instrument-manager")).toBeVisible({ timeout: 10_000 });
    // Wait for all 7 instruments to finish seeding (each fetches an MP3 from the dev server)
    await expect(page.locator(".instrument-list li")).toHaveCount(7, { timeout: 60_000 });
  });

  test("shows the instrument list", async ({ page }) => {
    const instruments = page.locator(".instrument-list li");
    await expect(instruments).toHaveCount(7);
  });

  test("+ Add button toggles the form", async ({ page }) => {
    const addButton = page.locator(".toggle-form-btn");
    await expect(addButton).toHaveText("+ Add");

    // Open the form
    await addButton.click();
    await expect(page.locator(".add-form")).toBeVisible();
    await expect(addButton).toHaveText("Cancel");

    // Close the form
    await addButton.click();
    await expect(page.locator(".add-form")).not.toBeVisible();
    await expect(addButton).toHaveText("+ Add");
  });

  test("upload button is disabled without name and file", async ({ page }) => {
    await page.locator(".toggle-form-btn").click();

    const uploadBtn = page.locator(".upload-btn");
    await expect(uploadBtn).toBeDisabled();
  });

  test("removing an instrument removes it from the list", async ({ page }) => {
    const instruments = page.locator(".instrument-list li");
    const initialCount = await instruments.count();

    // Remove the last instrument
    const lastRemoveBtn = page.locator(".remove-btn").last();
    await lastRemoveBtn.click();

    await expect(instruments).toHaveCount(initialCount - 1, { timeout: 5_000 });
  });
});
