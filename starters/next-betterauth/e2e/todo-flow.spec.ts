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

async function signIn(page: Page, credentials: { email: string; password: string }) {
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/dashboard", { timeout: TIMEOUT });
  await waitForApp(page);
}

async function signOut(page: Page) {
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL("/", { timeout: TIMEOUT });
  await expect(page.getByLabel("Email")).toBeVisible({ timeout: TIMEOUT });
}

test("todo persistence across sign-up→logout→login", async ({ page }) => {
  const runId = Date.now();
  const todo = `Todo ${runId}`;
  const credentials = {
    name: "Test User",
    email: `test-${runId}@example.com`,
    password: "testpassword",
  };

  await page.goto("/");
  await signUp(page, credentials);
  await expect(page.getByText(credentials.name)).toBeVisible({
    timeout: TIMEOUT,
  });

  await addTodo(page, todo);
  await expect(page.getByText(todo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });

  await signOut(page);
  await expect(page.getByText(todo)).not.toBeVisible();

  await signIn(page, credentials);
  await expect(page.getByText(credentials.name)).toBeVisible({
    timeout: TIMEOUT,
  });
  await expect(page.getByText(todo, { exact: true })).toHaveCount(1, { timeout: TIMEOUT });
});

test("transport loss preserves a locally saved delete", async ({ page }) => {
  const runId = Date.now();
  const credentials = {
    name: "Delete Failure User",
    email: `delete-failure-${runId}@example.com`,
    password: "testpassword",
  };
  const todo = `Recover this todo ${runId}`;
  let blockDelete = false;
  let droppedDeleteFrame = false;

  // Drop this mutation's transport while the starter waits only for local persistence.
  await page.context().routeWebSocket("**", (socket) => {
    const server = socket.connectToServer();
    socket.onMessage((message) => {
      if (blockDelete) {
        droppedDeleteFrame = true;
        void socket.close();
        return;
      }
      server.send(message);
    });
  });

  await page.goto("/");
  await signUp(page, credentials);
  await addTodo(page, todo);

  blockDelete = true;
  await page.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByText(todo, { exact: true })).not.toBeVisible({ timeout: TIMEOUT });
  const status = page.getByRole("status");
  await expect(status).toBeVisible();
  await expect(status).not.toContainText("Delete failed", { timeout: TIMEOUT });
  expect(droppedDeleteFrame).toBe(true);
  await expect(status).not.toContainText("Deleted");
  await expect(status).not.toContainText("Deleting…");
  blockDelete = false;
  await expect(page.getByText(todo, { exact: true })).not.toBeVisible();
});
