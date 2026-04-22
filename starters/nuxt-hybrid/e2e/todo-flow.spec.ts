import { expect, test, type Page } from "@playwright/test";

const TODO_INPUT_LABEL = "New todo";
const TIMEOUT = 20_000;

async function waitForApp(page: Page) {
  await expect(page.getByLabel(TODO_INPUT_LABEL)).toBeVisible({ timeout: TIMEOUT });
}

async function addTodo(page: Page, title: string) {
  await page.getByLabel(TODO_INPUT_LABEL).fill(title);
  await page.getByRole("button", { name: "Add" }).click();
  await expect(page.getByText(title)).toBeVisible({ timeout: TIMEOUT });
}

async function signUp(page: Page, credentials: { name: string; email: string; password: string }) {
  await page.goto("/signup");
  await page.getByLabel("Name").fill(credentials.name);
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);

  const [response] = await Promise.all([
    page.waitForResponse((r) => r.url().includes("/sign-up/email"), { timeout: TIMEOUT }),
    page.getByRole("button", { name: "Create account" }).click(),
  ]);

  if (!response.ok()) {
    const body = await response.text().catch(() => "(unreadable)");
    throw new Error(`Sign-up API responded ${response.status()}: ${body}`);
  }

  await expect(page).toHaveURL("/", { timeout: TIMEOUT });
  await waitForApp(page);
}

async function signIn(page: Page, credentials: { email: string; password: string }) {
  await page.goto("/signin");
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/", { timeout: TIMEOUT });
  await waitForApp(page);
}

async function signOut(page: Page) {
  await page.getByRole("button", { name: "Sign out" }).click();
  await waitForApp(page);
}

test("anonymous todo carries over to new account on sign-up", async ({ page }) => {
  const runId = Date.now();
  const todo = `Anonymous todo ${runId}`;
  const credentials = {
    name: "Test User",
    email: `test-${runId}@example.com`,
    password: "testpassword",
  };

  await page.goto("/");
  await waitForApp(page);
  await addTodo(page, todo);

  await signUp(page, credentials);
  await expect(page.getByText(todo)).toBeVisible({ timeout: TIMEOUT });

  await signOut(page);
  await expect(page.getByText(todo)).not.toBeVisible();

  await signIn(page, credentials);
  await expect(page.getByText(todo)).toBeVisible({ timeout: TIMEOUT });
});
