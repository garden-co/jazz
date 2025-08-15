import { clerk } from "@clerk/testing/playwright";
import { expect, test } from "@playwright/test";

test("login & expiration", async ({ page, context }) => {
  // Clear cookies first
  await context.clearCookies();

  await page.goto("/");

  // Clear storage after page loads to avoid security errors
  await page.evaluate(() => {
    try {
      localStorage.clear();
      sessionStorage.clear();
      // Clear IndexedDB
      if ("indexedDB" in window) {
        indexedDB.databases().then((databases) => {
          databases.forEach((db) => {
            if (db.name) indexedDB.deleteDatabase(db.name);
          });
        });
      }
    } catch (e) {
      console.log("Storage clear failed:", e);
    }
  });

  // Wait for page to load completely
  await page.waitForTimeout(3000);

  // Verify initial logged out state
  await expect(page.getByText("You're not logged in")).toBeVisible();

  // Manual login (works in our test environment)
  await page.getByRole("button", { name: "Sign in" }).click();
  await page.waitForTimeout(5000);

  await page
    .getByPlaceholder("Enter your email address")
    .waitFor({ timeout: 30000 });

  await page
    .getByPlaceholder("Enter your email address")
    .fill("guido+clerk-test@garden.co");

  await page.keyboard.press("Enter");

  await page
    .getByPlaceholder("Enter your password")
    .fill("guido+clerk-test@garden.co");

  await page.keyboard.press("Enter");

  await page.waitForURL("/");

  // Verify user is logged in
  await page.getByText("You're logged in").waitFor({ state: "visible" });
  expect(page.getByText("You're logged in")).toBeVisible();

  // Simulate expiration using clerk.signOut (ignore the warning about missing setup)
  await clerk.signOut({ page });

  // Navigate to home page to check logout state
  await page.goto("/");

  // Wait for logout to be processed and UI to update
  await page.getByText("You're not logged in").waitFor({ state: "visible" });
});
