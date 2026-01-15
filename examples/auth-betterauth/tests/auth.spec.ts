import { expect, test } from "@playwright/test";

// Generate unique email for each test run to avoid conflicts
const testId = Date.now();
const testEmail = `test-${testId}@example.com`;
const testPassword = "TestPassword123!";
const testName = "Test User";

test.describe("BetterAuth Sign Up", () => {
  test.beforeEach(async ({ page }) => {
    // Capture console output for debugging
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        console.log("CONSOLE ERROR:", msg.text());
      }
    });
    page.on("pageerror", (err) => console.log("PAGE ERROR:", err.message));

    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Jazz + BetterAuth Demo" }),
    ).toBeVisible({ timeout: 10000 });
  });

  test("displays sign in form by default", async ({ page }) => {
    await expect(page.getByRole("heading", { name: "Sign In" })).toBeVisible();
    await expect(page.getByPlaceholder("Email")).toBeVisible();
    await expect(page.getByPlaceholder("Password")).toBeVisible();
    await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Need an account\? Sign Up/ }),
    ).toBeVisible();
  });

  test("switches to sign up form", async ({ page }) => {
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();

    await expect(page.getByRole("heading", { name: "Sign Up" })).toBeVisible();
    await expect(page.getByPlaceholder("Name")).toBeVisible();
    await expect(page.getByPlaceholder("Email")).toBeVisible();
    await expect(page.getByPlaceholder("Password")).toBeVisible();
    await expect(page.getByRole("button", { name: "Sign Up" })).toBeVisible();
  });

  test("creates new account successfully", async ({ page }) => {
    // Switch to sign up
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await expect(page.getByRole("heading", { name: "Sign Up" })).toBeVisible();

    // Fill in the form
    await page.getByPlaceholder("Name").fill(testName);
    await page.getByPlaceholder("Email").fill(testEmail);
    await page.getByPlaceholder("Password").fill(testPassword);

    // Submit
    await page.getByRole("button", { name: "Sign Up" }).click();

    // Should be logged in and see user info
    await expect(page.getByText(`Logged in as: ${testName}`)).toBeVisible({
      timeout: 10000,
    });
    await expect(page.getByRole("button", { name: "Sign Out" })).toBeVisible();
  });

  test("shows error for duplicate email", async ({ browser }) => {
    const duplicateEmail = `duplicate-${testId}@example.com`;

    // Create first account in one context
    const context1 = await browser.newContext();
    const page1 = await context1.newPage();
    await page1.goto("/");
    await expect(
      page1.getByRole("heading", { name: "Jazz + BetterAuth Demo" }),
    ).toBeVisible({ timeout: 10000 });

    await page1
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await page1.getByPlaceholder("Name").fill("First User");
    await page1.getByPlaceholder("Email").fill(duplicateEmail);
    await page1.getByPlaceholder("Password").fill(testPassword);
    await page1.getByRole("button", { name: "Sign Up" }).click();
    await expect(page1.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });
    await context1.close();

    // Try to sign up with same email in a fresh context
    const context2 = await browser.newContext();
    const page2 = await context2.newPage();
    await page2.goto("/");
    await expect(
      page2.getByRole("heading", { name: "Jazz + BetterAuth Demo" }),
    ).toBeVisible({ timeout: 10000 });

    await page2
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await page2.getByPlaceholder("Name").fill("Second User");
    await page2.getByPlaceholder("Email").fill(duplicateEmail);
    await page2.getByPlaceholder("Password").fill(testPassword);
    await page2.getByRole("button", { name: "Sign Up" }).click();

    // Should show error for duplicate email
    await expect(page2.locator(".error")).toBeVisible({ timeout: 5000 });
    await context2.close();
  });
});

test.describe("BetterAuth Sign In", () => {
  const signInEmail = `signin-${testId}@example.com`;

  test.beforeAll(async ({ browser }) => {
    // Create an account to sign in with
    const page = await browser.newPage();
    await page.goto("/");
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await page.getByPlaceholder("Name").fill("Sign In Test User");
    await page.getByPlaceholder("Email").fill(signInEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign Up" }).click();
    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });
    await page.getByRole("button", { name: "Sign Out" }).click();
    await page.close();
  });

  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Jazz + BetterAuth Demo" }),
    ).toBeVisible({ timeout: 10000 });
  });

  test("signs in with valid credentials", async ({ page }) => {
    await page.getByPlaceholder("Email").fill(signInEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Logged in as: Sign In Test User")).toBeVisible(
      {
        timeout: 10000,
      },
    );
  });

  test("shows error for invalid credentials", async ({ page }) => {
    await page.getByPlaceholder("Email").fill(signInEmail);
    await page.getByPlaceholder("Password").fill("WrongPassword123!");
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.locator(".error")).toBeVisible({ timeout: 5000 });
  });

  test("shows error for non-existent email", async ({ page }) => {
    await page.getByPlaceholder("Email").fill("nonexistent@example.com");
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.locator(".error")).toBeVisible({ timeout: 5000 });
  });
});

test.describe("BetterAuth Sign Out", () => {
  const signOutEmail = `signout-${testId}@example.com`;

  test.beforeAll(async ({ browser }) => {
    // Create an account
    const page = await browser.newPage();
    await page.goto("/");
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await page.getByPlaceholder("Name").fill("Sign Out Test User");
    await page.getByPlaceholder("Email").fill(signOutEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign Up" }).click();
    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });
    await page.getByRole("button", { name: "Sign Out" }).click();
    await page.close();
  });

  test("signs out successfully", async ({ page }) => {
    await page.goto("/");

    // Sign in first
    await page.getByPlaceholder("Email").fill(signOutEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });

    // Sign out
    await page.getByRole("button", { name: "Sign Out" }).click();

    // Should be back to sign in form
    await expect(page.getByRole("heading", { name: "Sign In" })).toBeVisible({
      timeout: 5000,
    });
    await expect(
      page.getByRole("button", { name: "Sign Out" }),
    ).not.toBeVisible();
  });
});

test.describe("JWT Token Display", () => {
  const tokenEmail = `token-${testId}@example.com`;

  test.beforeAll(async ({ browser }) => {
    // Create an account
    const page = await browser.newPage();
    await page.goto("/");
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await page.getByPlaceholder("Name").fill("Token Test User");
    await page.getByPlaceholder("Email").fill(tokenEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign Up" }).click();
    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });
    await page.getByRole("button", { name: "Sign Out" }).click();
    await page.close();
  });

  test("displays JWT token after sign in", async ({ page }) => {
    await page.goto("/");

    await page.getByPlaceholder("Email").fill(tokenEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });

    // JWT token section should be visible
    await expect(
      page.getByRole("heading", { name: "JWT Token (for Jazz)" }),
    ).toBeVisible();

    // Token should be displayed (it's a long base64 string with dots)
    const tokenPre = page.locator("pre").first();
    const tokenText = await tokenPre.textContent();
    expect(tokenText).toMatch(/^eyJ/); // JWT tokens start with eyJ (base64 of {"...)
    expect(tokenText).toContain("."); // JWTs have dots separating header.payload.signature
  });

  test("displays decoded JWT claims", async ({ page }) => {
    await page.goto("/");

    await page.getByPlaceholder("Email").fill(tokenEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });

    // Decoded claims section should be visible
    await expect(
      page.getByRole("heading", { name: "Decoded Claims" }),
    ).toBeVisible();

    // Should show standard JWT claims
    await expect(page.getByText(/"sub":/)).toBeVisible();
    await expect(page.getByText(/"email":/)).toBeVisible();

    // Should contain our test email in the claims
    await expect(page.getByText(tokenEmail)).toBeVisible();
  });
});

test.describe("Example Jazz Policies Display", () => {
  const policyEmail = `policy-${testId}@example.com`;

  test.beforeAll(async ({ browser }) => {
    // Create an account
    const page = await browser.newPage();
    await page.goto("/");
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();
    await page.getByPlaceholder("Name").fill("Policy Test User");
    await page.getByPlaceholder("Email").fill(policyEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign Up" }).click();
    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });
    await page.getByRole("button", { name: "Sign Out" }).click();
    await page.close();
  });

  test("shows example Jazz policies when logged in", async ({ page }) => {
    await page.goto("/");

    await page.getByPlaceholder("Email").fill(policyEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });

    // Example policies section should be visible
    await expect(
      page.getByRole("heading", { name: "Example Jazz Policies" }),
    ).toBeVisible();

    // Should show policy syntax examples
    await expect(page.getByText("CREATE POLICY")).toBeVisible();
    await expect(page.getByText("@viewer")).toBeVisible();
    await expect(
      page.getByText("@viewer.claims.subscriptionTier"),
    ).toBeVisible();
  });

  test("shows groove-server configuration example", async ({ page }) => {
    await page.goto("/");

    await page.getByPlaceholder("Email").fill(policyEmail);
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Logged in as:")).toBeVisible({
      timeout: 10000,
    });

    // Config section should be visible
    await expect(
      page.getByRole("heading", { name: "groove-server Configuration" }),
    ).toBeVisible();

    // Should show TOML config example
    await expect(page.getByText(/\[auth\]/)).toBeVisible();
    await expect(page.getByText(/jwks_url/)).toBeVisible();
  });
});

test.describe("Form Validation", () => {
  test("requires email field", async ({ page }) => {
    await page.goto("/");

    // Try to submit without email
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign In" }).click();

    // Form should not submit (email is required)
    // The form uses HTML5 validation, so we check we're still on the form
    await expect(page.getByRole("heading", { name: "Sign In" })).toBeVisible();
  });

  test("requires password field", async ({ page }) => {
    await page.goto("/");

    // Try to submit without password
    await page.getByPlaceholder("Email").fill("test@example.com");
    await page.getByRole("button", { name: "Sign In" }).click();

    // Form should not submit (password is required)
    await expect(page.getByRole("heading", { name: "Sign In" })).toBeVisible();
  });

  test("requires name field on sign up", async ({ page }) => {
    await page.goto("/");
    await page
      .getByRole("button", { name: /Need an account\? Sign Up/ })
      .click();

    // Try to submit without name
    await page.getByPlaceholder("Email").fill("test@example.com");
    await page.getByPlaceholder("Password").fill(testPassword);
    await page.getByRole("button", { name: "Sign Up" }).click();

    // Form should not submit (name is required)
    await expect(page.getByRole("heading", { name: "Sign Up" })).toBeVisible();
  });
});

test.describe("Loading State", () => {
  test("shows loading state or sign in form on page load", async ({ page }) => {
    // Don't wait for full load
    await page.goto("/", { waitUntil: "commit" });

    // Either loading is shown briefly or the form appears
    // This tests that the app handles initial state properly
    const loadingText = page.getByText("Loading...");
    const signInHeading = page.getByRole("heading", { name: "Sign In" });

    // Wait for either element to be visible
    await expect(loadingText.or(signInHeading)).toBeVisible({ timeout: 10000 });
  });
});
