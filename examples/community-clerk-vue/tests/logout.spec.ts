import { expect, test } from "@playwright/test";

test("login & logout", async ({ page }) => {
  // Capture console messages
  page.on("console", (msg) => {
    console.log(`[BROWSER ${msg.type().toUpperCase()}]:`, msg.text());
  });

  // Capture network failures
  page.on("requestfailed", (request) => {
    console.log(
      `[NETWORK FAILED]:`,
      request.url(),
      request.failure()?.errorText,
    );
  });

  await page.goto("/");

  // Wait for page to load completely
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
    .waitFor({ timeout: 15000 });

  await page
    .getByPlaceholder("Enter your email address")
    .fill("guido+clerk-test@garden.co");

  await page.keyboard.press("Enter");

  await page
    .getByPlaceholder("Enter your password")
    .fill("guido+clerk-test@garden.co");

  console.log("Pressing Enter to submit password...");
  await page.keyboard.press("Enter");

  console.log("Waiting for navigation to /...");
  await page.waitForURL("/", { timeout: 60000 });

  await page.getByText("You're logged in").waitFor({ state: "visible" });

  expect(page.getByText("You're logged in")).toBeVisible();

  await page.getByRole("button", { name: "Logout" }).click();

  await page.getByText("You're not logged in").waitFor({ state: "visible" });

  expect(page.getByText("You're not logged in")).toBeVisible();
});
