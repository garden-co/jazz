import { expect, test } from "@playwright/test";

const TIMEOUT = 60_000;
const CHAT = process.env.NEXT_PUBLIC_CHAT_ID || "chat-01";

test("a new account posts in the chat and the message survives a reload", async ({ page }) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByRole("button", { name: "Sign up" }).click();
  await page.getByLabel("Email").fill(`ada-${run}@example.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByTestId("auth-submit").click();
  await expect(page.getByTestId("logout-button")).toBeVisible({ timeout: TIMEOUT });

  // A member writes to the general chat; the thin client shows it once the
  // backend has it.
  const text = `Hello from ${run}`;
  await page.getByTestId(`message-input-${CHAT}`).fill(text);
  await page.getByTestId(`send-button-${CHAT}`).click();
  const list = page.getByTestId(`message-list-${CHAT}`);
  await expect(list.getByText(text)).toBeVisible({ timeout: TIMEOUT });

  await page.reload();
  await expect(page.getByTestId(`message-list-${CHAT}`).getByText(text)).toBeVisible({
    timeout: TIMEOUT,
  });
});
