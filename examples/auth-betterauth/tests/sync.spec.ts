import { type Page, expect, test } from "@playwright/test";

/**
 * E2E tests for authenticated sync with policy-filtered data.
 *
 * These tests verify that:
 * 1. Authenticated users can connect to groove-server with JWT tokens
 * 2. Users can create documents via sync
 * 3. Policy filtering ensures users only see their own documents
 *
 * Prerequisites:
 * - BetterAuth server running on localhost:3001
 * - groove-server running on localhost:8080 (with JWT validation configured)
 * - Vite dev server running on localhost:5173
 */

// Generate unique identifiers for each test run
const testId = Date.now();

// Test user credentials
const user1 = {
  email: `sync-user1-${testId}@example.com`,
  password: "TestPassword123!",
  name: "Sync User One",
};

const user2 = {
  email: `sync-user2-${testId}@example.com`,
  password: "TestPassword123!",
  name: "Sync User Two",
};

// Helper to sign up a new user
async function signUp(
  page: Page,
  email: string,
  password: string,
  name: string,
) {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Jazz + BetterAuth Demo" }),
  ).toBeVisible({ timeout: 10000 });

  await page.getByRole("button", { name: /Need an account\? Sign Up/ }).click();
  await page.getByPlaceholder("Name").fill(name);
  await page.getByPlaceholder("Email").fill(email);
  await page.getByPlaceholder("Password").fill(password);
  await page.getByRole("button", { name: "Sign Up" }).click();

  await expect(page.getByText(`Logged in as: ${name}`)).toBeVisible({
    timeout: 10000,
  });
}

// Helper to sign in an existing user
async function signIn(page: Page, email: string, password: string) {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Jazz + BetterAuth Demo" }),
  ).toBeVisible({ timeout: 10000 });

  await page.getByPlaceholder("Email").fill(email);
  await page.getByPlaceholder("Password").fill(password);
  await page.getByRole("button", { name: "Sign In" }).click();

  await expect(page.getByText("Logged in as:")).toBeVisible({
    timeout: 10000,
  });
}

// Helper to connect to sync server
async function connectToSync(page: Page) {
  // Wait for SyncTest component to be ready
  await expect(page.getByTestId("sync-status")).toContainText("Ready", {
    timeout: 15000,
  });

  // Click connect
  await page.getByTestId("connect-btn").click();

  // Wait for connected state
  await expect(page.getByTestId("sync-status")).toContainText("Ready", {
    timeout: 15000,
  });
}

// Helper to create a document
async function createDocument(page: Page, title: string) {
  await page.getByTestId("doc-title-input").fill(title);
  await page.getByTestId("create-doc-btn").click();

  // Wait for the document to appear in the list
  await expect(page.getByTestId("documents-list")).toContainText(title, {
    timeout: 10000,
  });
}

test.describe("Authenticated Sync", () => {
  // Run tests sequentially since they share server state
  test.describe.configure({ mode: "serial" });

  test.beforeEach(async ({ page }) => {
    // Capture console errors for debugging
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        console.log("CONSOLE ERROR:", msg.text());
      }
    });
    page.on("pageerror", (err) => console.log("PAGE ERROR:", err.message));
  });

  test("shows sync test component when logged in", async ({ page }) => {
    await signUp(page, user1.email, user1.password, user1.name);

    // SyncTest component should be visible
    await expect(page.getByRole("heading", { name: "Sync Test" })).toBeVisible({
      timeout: 10000,
    });
    await expect(page.getByTestId("sync-status")).toBeVisible();
    await expect(page.getByTestId("connect-btn")).toBeVisible();
  });

  test("connects to sync server with JWT token", async ({ page }) => {
    await signIn(page, user1.email, user1.password);
    await connectToSync(page);

    // Should show connected status
    await expect(page.getByTestId("sync-status")).toContainText("Ready");
  });

  test("creates and displays documents", async ({ page }) => {
    await signIn(page, user1.email, user1.password);
    await connectToSync(page);

    // Create a document
    const docTitle = `User1 Doc ${Date.now()}`;
    await createDocument(page, docTitle);

    // Document should be visible
    await expect(page.getByTestId("documents-list")).toContainText(docTitle);
  });
});

test.describe("Policy-Filtered Sync", () => {
  // Run tests sequentially
  test.describe.configure({ mode: "serial" });

  // Increase timeout for multi-user sync tests
  test.setTimeout(60000);

  test.beforeEach(async ({ page }) => {
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        console.log("CONSOLE ERROR:", msg.text());
      }
    });
  });

  test("users only see their own documents", async ({ browser }) => {
    // Create two separate browser contexts (simulating two different users)
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();
    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    try {
      // Sign up both users
      await signUp(page1, user1.email, user1.password, user1.name);
      await signUp(page2, user2.email, user2.password, user2.name);

      // Connect both to sync server
      await connectToSync(page1);
      await connectToSync(page2);

      // User 1 creates a document
      const doc1Title = `User1 Private Doc ${Date.now()}`;
      await createDocument(page1, doc1Title);

      // User 2 creates a document
      const doc2Title = `User2 Private Doc ${Date.now()}`;
      await createDocument(page2, doc2Title);

      // Small wait for sync to complete
      await page1.waitForTimeout(1000);
      await page2.waitForTimeout(1000);

      // User 1 should see their doc but NOT User 2's doc
      await expect(page1.getByTestId("documents-list")).toContainText(
        doc1Title,
      );
      await expect(page1.getByTestId("documents-list")).not.toContainText(
        doc2Title,
      );

      // User 2 should see their doc but NOT User 1's doc
      await expect(page2.getByTestId("documents-list")).toContainText(
        doc2Title,
      );
      await expect(page2.getByTestId("documents-list")).not.toContainText(
        doc1Title,
      );
    } finally {
      await context1.close();
      await context2.close();
    }
  });

  test("new documents from same user sync across tabs", async ({ browser }) => {
    // Same user in two tabs (same browser context = same cookies)
    const context = await browser.newContext();
    const page1 = await context.newPage();
    const page2 = await context.newPage();

    try {
      // Sign in on first page
      await signIn(page1, user1.email, user1.password);
      await connectToSync(page1);

      // Navigate second page (same session, same user)
      await page2.goto("/");
      await expect(page2.getByText("Logged in as:")).toBeVisible({
        timeout: 10000,
      });
      await connectToSync(page2);

      // Create document on page 1
      const docTitle = `Synced Doc ${Date.now()}`;
      await createDocument(page1, docTitle);

      // Wait for sync
      await page2.waitForTimeout(2000);

      // Page 2 should receive the document via sync
      await expect(page2.getByTestId("documents-list")).toContainText(
        docTitle,
        { timeout: 10000 },
      );
    } finally {
      await context.close();
    }
  });

  test("document isolation persists after reconnection", async ({
    browser,
  }) => {
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();
    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    try {
      // Sign in both users
      await signIn(page1, user1.email, user1.password);
      await signIn(page2, user2.email, user2.password);

      // User 1 connects, creates doc, disconnects
      await connectToSync(page1);
      const doc1Title = `Persisted Doc ${Date.now()}`;
      await createDocument(page1, doc1Title);

      // Wait for sync to complete
      await page1.waitForTimeout(1000);

      // User 2 connects - should NOT see User 1's document
      await connectToSync(page2);
      await page2.waitForTimeout(2000);

      // Verify isolation
      await expect(page2.getByTestId("documents-list")).not.toContainText(
        doc1Title,
      );

      // User 1's document should still be visible to them
      await expect(page1.getByTestId("documents-list")).toContainText(
        doc1Title,
      );
    } finally {
      await context1.close();
      await context2.close();
    }
  });
});
