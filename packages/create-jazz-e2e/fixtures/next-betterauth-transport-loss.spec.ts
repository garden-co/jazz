import { expect, test, type Page } from "@playwright/test";

const TODO_INPUT_LABEL = "New todo";
const TIMEOUT = 20_000;

async function waitForApp(page: Page) {
  await expect(page.getByLabel(TODO_INPUT_LABEL)).toBeVisible({
    timeout: TIMEOUT,
  });
}

async function addTodo(page: Page, title: string) {
  await page.getByLabel(TODO_INPUT_LABEL).fill(title);
  await page.getByRole("button", { name: "Add" }).click();
  await expect(page.getByText(title, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
  await expect(page.getByRole("status")).toContainText("Saved locally", { timeout: TIMEOUT });
}

async function signUp(page: Page, credentials: { name: string; email: string; password: string }) {
  const [response] = await Promise.all([
    page.waitForResponse((r) => r.url().includes("/sign-up/email"), {
      timeout: TIMEOUT,
    }),
    (async () => {
      await page.getByRole("button", { name: "Create an account" }).click();
      await page.getByLabel("Name").fill(credentials.name);
      await page.getByLabel("Email").fill(credentials.email);
      await page.getByLabel("Password").fill(credentials.password);
      await page.getByRole("button", { name: "Create account" }).click();
    })(),
  ]);

  if (!response.ok()) {
    const body = await response.text().catch(() => "(unreadable)");
    throw new Error(`Sign-up API responded ${response.status()}: ${body}`);
  }

  await expect(page).toHaveURL("/dashboard", { timeout: TIMEOUT });
  await waitForApp(page);
}

test("transport loss preserves a locally saved delete", async ({ page, browser }) => {
  const runId = Date.now();
  const credentials = {
    name: "Delete Failure User",
    email: `delete-failure-${runId}@example.com`,
    password: "testpassword",
  };
  const todo = `Recover this todo ${runId}`;
  const retainedTodo = `Retain this todo ${runId}`;
  // The production transport lives in a SharedWorker, outside page WebSocket routing.
  const controlUrl = process.env.JAZZ_E2E_TRANSPORT_CONTROL_URL;
  if (!controlUrl) throw new Error("Run this transport test through create-jazz-e2e");
  const control = async (method = "GET") => {
    const response = await page.request.fetch(controlUrl, { method });
    expect(response.ok()).toBe(true);
    return (await response.json()) as {
      blocked: boolean;
      droppedBytes: number;
    };
  };

  await page.goto("/");
  await signUp(page, credentials);
  await addTodo(page, todo);
  await addTodo(page, retainedTodo);

  // A fresh storage context learns both rows through the server, not the writer's local DB.
  const observerContext = await browser.newContext({ baseURL: new URL(page.url()).origin });
  try {
    const observer = await observerContext.newPage();
    await observer.goto("/");
    await observer.getByLabel("Email").fill(credentials.email);
    await observer.getByLabel("Password").fill(credentials.password);
    await observer.getByRole("button", { name: "Sign in" }).click();
    await expect(observer).toHaveURL("/dashboard", { timeout: TIMEOUT });
    await waitForApp(observer);
    await expect(observer.getByText(todo, { exact: true })).toBeVisible({ timeout: TIMEOUT });
    await expect(observer.getByText(retainedTodo, { exact: true })).toBeVisible({
      timeout: TIMEOUT,
    });

    await control("POST");
    await page
      .getByRole("listitem")
      .filter({ hasText: todo })
      .getByRole("button", { name: "Delete" })
      .click();
    await expect(page.getByText(todo, { exact: true })).not.toBeVisible({ timeout: TIMEOUT });
    const status = page.getByRole("status");
    // Require an actual dropped outbound transmission, independent of worker ownership.
    await expect
      .poll(async () => (await control()).droppedBytes, { timeout: TIMEOUT })
      .toBeGreaterThan(0);
    await expect(status).toHaveText("Saved locally", { timeout: TIMEOUT });
    await expect(status).not.toContainText("Delete failed");
    await expect(status).not.toContainText("Deleted");
    await expect(status).not.toContainText("Deleting…");
    await control("DELETE");
    // Prove the exact deletion reaches another replica after transport recovery.
    await expect(observer.getByText(todo, { exact: true })).not.toBeVisible({ timeout: TIMEOUT });
    await expect(observer.getByText(retainedTodo, { exact: true })).toBeVisible({
      timeout: TIMEOUT,
    });
    await expect(page.getByText(todo, { exact: true })).not.toBeVisible();
    await expect(status).toHaveText("Saved locally");
  } finally {
    await control("DELETE");
    await observerContext.close();
  }
});
