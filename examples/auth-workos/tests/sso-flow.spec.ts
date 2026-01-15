import { type Page, expect, test } from "@playwright/test";

/**
 * Full SSO Flow Tests
 *
 * These tests require WorkOS test credentials to be set as environment variables:
 * - WORKOS_TEST_EMAIL: The test user's email
 * - WORKOS_TEST_PASSWORD: The test user's password
 *
 * Run with: WORKOS_TEST_EMAIL=user@example.com WORKOS_TEST_PASSWORD=secret npm test
 */

const testEmail = process.env.WORKOS_TEST_EMAIL;
const testPassword = process.env.WORKOS_TEST_PASSWORD;

const hasCredentials = testEmail && testPassword;

/**
 * Helper to complete the full SSO authentication flow
 */
async function completeAuthFlow(page: Page): Promise<void> {
  // Load the app
  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Sign in with SSO" }),
  ).toBeVisible({ timeout: 10000 });

  // Click sign in - redirects to AuthKit
  await page.getByRole("button", { name: "Sign in with SSO" }).click();

  // Fill email on AuthKit page
  await page.waitForURL(/authkit\.app/, { timeout: 10000 });
  await page.getByPlaceholder("Your email address").fill(testEmail!);
  await page.getByRole("button", { name: "Continue" }).click();

  // Handle password page (could be authkit.app or signin.workos.com)
  await page.waitForSelector('input[type="password"]', { timeout: 15000 });

  // Fill email again if needed (some flows require it)
  const emailInput = page.getByPlaceholder("Your email address");
  if (await emailInput.isVisible().catch(() => false)) {
    await emailInput.fill(testEmail!);
    await page.getByRole("button", { name: "Continue" }).click();
    await page.waitForSelector('input[type="password"]', { timeout: 10000 });
  }

  // Fill password and sign in
  await page.getByPlaceholder("Your password").fill(testPassword!);
  await page.getByRole("button", { name: "Sign in" }).click();

  // Wait for redirect back to app - use longer timeout and handle potential errors
  try {
    await page.waitForURL(/localhost:5174/, { timeout: 30000 });
  } catch {
    // Log current state for debugging
    console.log("Redirect failed. Current URL:", page.url());
    const bodyText = await page
      .locator("body")
      .innerText()
      .catch(() => "");
    console.log("Page content:", bodyText.slice(0, 500));
    throw new Error(`Failed to redirect to app. Current URL: ${page.url()}`);
  }

  // Wait for authenticated state
  await expect(page.getByText("Logged in via WorkOS SSO")).toBeVisible({
    timeout: 10000,
  });
}

test.describe("WorkOS Full SSO Flow", () => {
  // Run serially to avoid rate limiting issues with WorkOS
  test.describe.configure({ mode: "serial" });

  test.skip(
    !hasCredentials,
    "Skipping SSO flow tests - set WORKOS_TEST_EMAIL and WORKOS_TEST_PASSWORD env vars",
  );

  test("completes full SSO authentication flow", async ({ page }) => {
    test.setTimeout(60000);

    await completeAuthFlow(page);

    // Verify authenticated state
    await expect(page.getByRole("button", { name: "Sign Out" })).toBeVisible();
  });

  test("displays user info after authentication", async ({ page }) => {
    test.setTimeout(60000);

    await completeAuthFlow(page);

    // Verify user info is displayed
    await expect(page.getByText(`Email: ${testEmail}`)).toBeVisible();
    await expect(page.getByText(/User ID: user_/)).toBeVisible();
  });

  test("displays access token and decoded claims", async ({ page }) => {
    test.setTimeout(60000);

    await completeAuthFlow(page);

    // Verify token section
    await expect(
      page.getByRole("heading", { name: "WorkOS Access Token" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Raw Token" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Decoded Claims" }),
    ).toBeVisible();

    // Verify JWT is displayed (starts with eyJ)
    const tokenPre = page.locator("pre").first();
    await expect(tokenPre).toContainText("eyJ");

    // Verify claims table shows expected fields
    await expect(page.getByText(/"sub":/)).toBeVisible();
    await expect(page.getByText(/"org_id":/)).toBeVisible();
    await expect(page.getByText(/"role":/)).toBeVisible();
  });

  test("shows policy examples after authentication", async ({ page }) => {
    test.setTimeout(60000);

    await completeAuthFlow(page);

    // Verify policy examples section
    await expect(
      page.getByRole("heading", { name: "Jazz Policies with WorkOS Claims" }),
    ).toBeVisible();
    await expect(page.getByText("@viewer.claims.org_id").first()).toBeVisible();
    await expect(
      page.getByText("@viewer.claims.permissions").first(),
    ).toBeVisible();
    await expect(page.getByText("@viewer.claims.role").first()).toBeVisible();
  });

  test("can sign out after authentication", async ({ page }) => {
    test.setTimeout(60000);

    await completeAuthFlow(page);

    // Sign out
    await page.getByRole("button", { name: "Sign Out" }).click();

    // Wait for sign out to complete - WorkOS may redirect through their logout flow
    await page.waitForTimeout(3000);

    // After sign out, we should either be at the app's sign-in page
    // or redirected through WorkOS logout
    // The key indicator is that we're no longer authenticated
    const isAuthenticated = await page
      .getByText("Logged in via WorkOS SSO")
      .isVisible()
      .catch(() => false);

    expect(isAuthenticated).toBe(false);

    // If we're back at the app, sign in button should be visible
    // If not, we might be on WorkOS logout page which is also acceptable
    const currentUrl = page.url();
    if (currentUrl.includes("localhost:5174")) {
      await expect(
        page.getByRole("button", { name: "Sign in with SSO" }),
      ).toBeVisible({ timeout: 5000 });
    }
  });
});
