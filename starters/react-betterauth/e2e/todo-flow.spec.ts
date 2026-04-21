import { expect, test, type Page } from "@playwright/test";

const TODO_INPUT_LABEL = "New todo";
const TIMEOUT = 20_000;

async function signUp(page: Page, email: string, password: string, name: string) {
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Create account" }).click();
}

async function signIn(page: Page, email: string, password: string) {
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
  await expect(page.getByText(title)).toBeVisible({ timeout: TIMEOUT });
}

test("signup → add todo → reload → todo persists", async ({ page }) => {
  const runId = Date.now();
  const email = `alice-${runId}@example.com`;
  const password = "s3cr3tpassword";
  const todo = `Buy milk ${runId}`;

  await page.goto("/");
  await signUp(page, email, password, "Alice");
  await waitForTodoApp(page);
  await addTodo(page, todo);

  await page.reload();
  await waitForTodoApp(page);
  await expect(page.getByText(todo)).toBeVisible({ timeout: TIMEOUT });
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

  await page.goto("/");
  await waitForTodoApp(page);
  await expect(page.getByText(todo)).toBeVisible({ timeout: TIMEOUT });
});
