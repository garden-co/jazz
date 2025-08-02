import { expect, test } from "@playwright/test";

test("login & reload", async ({ page, context }) => {
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

  // Wait for page to load completely (like the working tests)
  await page.waitForTimeout(3000);

  // Wait for the page to load and show the logged out state
  await expect(page.getByText("You're not logged in")).toBeVisible();

  // Click sign in and wait for Clerk modal to appear
  await page.getByRole("button", { name: "Sign in" }).click();

  // Wait a bit for Clerk to initialize and show the form
  await page.waitForTimeout(5000);

  // Wait for Clerk to load and show the email input with a longer timeout
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

  await page.getByText("You're logged in").waitFor({ state: "visible" });

  expect(page.getByText("You're logged in")).toBeVisible();

  await page.reload();

  await page.getByText("You're logged in").waitFor({ state: "visible" });

  expect(page.getByText("You're logged in")).toBeVisible();
});
