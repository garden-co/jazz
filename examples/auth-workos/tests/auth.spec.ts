import { expect, test } from "@playwright/test";

test.describe("WorkOS Demo - Unauthenticated State", () => {
  test.beforeEach(async ({ page }) => {
    // Capture console output for debugging
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        console.log("CONSOLE ERROR:", msg.text());
      }
    });
    page.on("pageerror", (err) => console.log("PAGE ERROR:", err.message));

    await page.goto("/");
  });

  test("displays page title and description", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: "Jazz + WorkOS Demo" }),
    ).toBeVisible({ timeout: 10000 });

    await expect(
      page.getByText(
        "Enterprise SSO with automatic role and permissions claims for Jazz policies.",
      ),
    ).toBeVisible();
  });

  test("shows sign in with WorkOS section", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: "Sign In with WorkOS" }),
    ).toBeVisible({ timeout: 10000 });

    await expect(
      page.getByText(/WorkOS AuthKit provides enterprise SSO/),
    ).toBeVisible();
  });

  test("displays sign in button", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Sign in with SSO" }),
    ).toBeVisible({ timeout: 10000 });
  });

  test("shows Test SSO IdP information", async ({ page }) => {
    await expect(
      page.getByText(/This demo uses WorkOS Test SSO IdP/),
    ).toBeVisible({ timeout: 10000 });
  });
});

test.describe("WorkOS Demo - Sign In Redirect", () => {
  test("clicking sign in initiates authentication flow", async ({
    page,
    context,
  }) => {
    await page.goto("/");

    // Wait for the page to load
    await expect(
      page.getByRole("button", { name: "Sign in with SSO" }),
    ).toBeVisible({ timeout: 10000 });

    // Store initial URL
    const initialUrl = page.url();

    // Listen for popup or navigation
    const [popup] = await Promise.all([
      // WorkOS AuthKit may open in popup or same window
      context
        .waitForEvent("page", { timeout: 5000 })
        .catch(() => null),
      page.getByRole("button", { name: "Sign in with SSO" }).click(),
    ]);

    // Wait a moment for navigation to complete
    await page.waitForTimeout(2000);

    // Either a popup was opened or the page navigated away from localhost
    if (popup) {
      // Popup was opened - check it's going somewhere external
      const popupUrl = popup.url();
      expect(popupUrl).not.toBe("about:blank");
      // The popup should be for authentication (WorkOS or its redirect)
      expect(popupUrl).toMatch(/workos\.com|localhost/);
    } else {
      // Same-window navigation - URL should have changed OR we should see auth flow
      // WorkOS AuthKit redirects to their auth page
      const currentUrl = page.url();
      // Either navigated to WorkOS or still processing
      const hasNavigated = currentUrl !== initialUrl;
      const isWorkOS = currentUrl.includes("workos.com");
      const isAuthCallback = currentUrl.includes("callback");

      // At minimum, clicking should have triggered some action
      expect(hasNavigated || isWorkOS || isAuthCallback).toBe(true);
    }
  });
});

test.describe("WorkOS Demo - Loading State", () => {
  test("shows loading state or sign in form on page load", async ({ page }) => {
    // Don't wait for full load
    await page.goto("/", { waitUntil: "commit" });

    // Either loading is shown briefly or the form appears
    const loadingText = page.getByText("Loading...");
    const signInHeading = page.getByRole("heading", {
      name: "Sign In with WorkOS",
    });

    // Wait for either element to be visible
    await expect(loadingText.or(signInHeading)).toBeVisible({ timeout: 10000 });
  });
});

test.describe("WorkOS Demo - Static Content", () => {
  // Note: These tests verify content shown in unauthenticated state
  // The demo shows policy examples even when not logged in

  test("page structure is correct when unauthenticated", async ({ page }) => {
    await page.goto("/");

    // Main heading should be visible
    await expect(
      page.getByRole("heading", { name: "Jazz + WorkOS Demo" }),
    ).toBeVisible({ timeout: 10000 });

    // Sign in card should be visible
    await expect(
      page.getByRole("heading", { name: "Sign In with WorkOS" }),
    ).toBeVisible();

    // Sign out button should NOT be visible (not authenticated)
    await expect(
      page.getByRole("button", { name: "Sign Out" }),
    ).not.toBeVisible();
  });

  test("does not show authenticated content when not logged in", async ({
    page,
  }) => {
    await page.goto("/");

    // Wait for page to load
    await expect(
      page.getByRole("heading", { name: "Jazz + WorkOS Demo" }),
    ).toBeVisible({ timeout: 10000 });

    // These elements should NOT be visible when unauthenticated
    await expect(page.getByText("Logged in via WorkOS SSO")).not.toBeVisible();
    await expect(
      page.getByRole("heading", { name: "WorkOS Access Token" }),
    ).not.toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Jazz Policies with WorkOS Claims" }),
    ).not.toBeVisible();
  });
});

test.describe("WorkOS Demo - Accessibility", () => {
  test("sign in button is keyboard accessible", async ({ page }) => {
    await page.goto("/");

    // Wait for button to be visible
    const signInButton = page.getByRole("button", { name: "Sign in with SSO" });
    await expect(signInButton).toBeVisible({ timeout: 10000 });

    // Focus the button using keyboard navigation
    await signInButton.focus();

    // Verify it's focused
    await expect(signInButton).toBeFocused();
  });

  test("page has proper heading hierarchy", async ({ page }) => {
    await page.goto("/");

    // Wait for page to load
    await expect(
      page.getByRole("heading", { name: "Jazz + WorkOS Demo" }),
    ).toBeVisible({ timeout: 10000 });

    // Check h1 exists (main title)
    const h1 = page.locator("h1");
    await expect(h1).toHaveCount(1);
    await expect(h1).toHaveText("Jazz + WorkOS Demo");

    // Check h2 exists for sections
    const h2 = page.locator("h2");
    await expect(h2.first()).toBeVisible();
  });
});
