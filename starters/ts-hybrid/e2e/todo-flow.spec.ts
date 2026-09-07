import { expect, test, type Page } from "@playwright/test";

const TODO_INPUT_LABEL = "New todo";
const TIMEOUT = 20_000;

async function signUp(page: Page, email: string, password: string, name: string) {
  await page.getByRole("button", { name: "Sign up" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Create account" }).click();
}

async function signIn(page: Page, email: string, password: string) {
  await page.getByRole("button", { name: "Sign in" }).click();
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
}

async function waitForTodoApp(page: Page) {
  await expect(page.getByLabel(TODO_INPUT_LABEL)).toBeVisible({ timeout: TIMEOUT });
}

async function addTodo(page: Page, title: string) {
  await page.getByLabel(TODO_INPUT_LABEL).fill(title);
  await page.getByRole("button", { name: "Add" }).click();
  await expect(page.getByText(title, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
  await expect(page.getByRole("status")).toContainText("Saved locally", { timeout: TIMEOUT });
}

test("signup → add todo → reload → todo persists", async ({ page }) => {
  const runId = Date.now();
  const email = `alice-${runId}@example.com`;
  const password = "s3cr3tpassword";
  const todo = `Buy milk ${runId}`;

  await page.goto("/");
  await waitForTodoApp(page);
  const localTodo = `Before signup ${runId}`;
  await addTodo(page, localTodo);
  const linked = page.waitForResponse(
    (response) =>
      /\/accounts\/links\/accept(?:\?|$)/.test(response.url()) &&
      response.request().method() === "POST",
  );
  await signUp(page, email, password, "Alice");
  expect((await linked).ok()).toBe(true);
  await waitForTodoApp(page);
  await expect(page.getByText(localTodo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
  await addTodo(page, todo);

  await page.reload();
  await waitForTodoApp(page);
  await expect(page.getByText(todo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });

  // Complete the session switch, then re-admit the provider identity. A provider
  // session alone must not make a pending/failed Jazz link look successful.
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await waitForTodoApp(page);
  await expect(page.getByText(localTodo, { exact: true })).toHaveCount(0, { timeout: TIMEOUT });
  await signIn(page, email, password);
  await waitForTodoApp(page);
  await expect(page.getByText(localTodo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
  await expect(page.getByText(todo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
});

test("signin with existing account shows todos", async ({ page }) => {
  const runId = Date.now();
  const email = `bob-${runId}@example.com`;
  const password = "s3cr3tpassword";
  const todo = `Walk the dog ${runId}`;

  await page.goto("/");
  await signUp(page, email, password, "Bob");
  await waitForTodoApp(page);
  await addTodo(page, todo);

  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await waitForTodoApp(page);
  await expect(page.getByText(todo, { exact: true })).toHaveCount(0, { timeout: TIMEOUT });
  await signIn(page, email, password);
  await waitForTodoApp(page);
  await expect(page.getByText(todo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
});

test("a separate account sees only its own todos", async ({ page, browser }) => {
  const runId = Date.now();
  await page.goto("/");
  await waitForTodoApp(page);
  const ownerTodo = `Private owner todo ${runId}`;
  await addTodo(page, ownerTodo);
  const linked = page.waitForResponse(
    (response) =>
      /\/accounts\/links\/accept(?:\?|$)/.test(response.url()) &&
      response.request().method() === "POST",
  );
  await signUp(page, `owner-${runId}@example.com`, "s3cr3tpassword", "Owner");
  expect((await linked).ok()).toBe(true);
  await waitForTodoApp(page);
  await expect(page.getByText(ownerTodo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });

  const otherContext = await browser.newContext();
  try {
    const other = await otherContext.newPage();
    await other.goto(page.url());
    await waitForTodoApp(other);
    const otherLinked = other.waitForResponse(
      (response) =>
        /\/accounts\/links\/accept(?:\?|$)/.test(response.url()) &&
        response.request().method() === "POST",
    );
    await signUp(other, `other-${runId}@example.com`, "s3cr3tpassword", "Other");
    expect((await otherLinked).ok()).toBe(true);
    await waitForTodoApp(other);
    const otherTodo = `Private other todo ${runId}`;
    await addTodo(other, otherTodo);
    // Reload to exercise the server's published policies with a new subscription.
    await other.reload();
    await waitForTodoApp(other);
    await expect(other.getByText(otherTodo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
    await expect(other.getByText(ownerTodo, { exact: true })).toHaveCount(0);
    await page.reload();
    await waitForTodoApp(page);
    await expect(page.getByText(ownerTodo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
    await expect(page.getByText(otherTodo, { exact: true })).toHaveCount(0);
  } finally {
    await otherContext.close();
  }
});
