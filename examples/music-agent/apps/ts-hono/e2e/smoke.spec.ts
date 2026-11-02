import { expect, test } from "@playwright/test";

const TIMEOUT = 90_000;

test("the agent answers the seeded request and a follow-up, and both survive a reload", async ({
  page,
}) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill("Ada");
  await page.getByLabel("Email").fill(`ada-${run}@music-agent.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  // The first conversation seeds itself and the scripted agent answers it on
  // the server; a finished reply offers "Regenerate".
  const regenerate = page.getByRole("button", { name: "Regenerate" });
  await expect(regenerate.first()).toBeVisible({ timeout: TIMEOUT });

  const question = `Which venues are free in May? (${run})`;
  await page.getByRole("textbox", { name: "Message input" }).fill(question);
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(page.getByText(question)).toBeVisible({ timeout: TIMEOUT });
  await expect(regenerate).toHaveCount(2, { timeout: TIMEOUT });

  await page.reload();
  await expect(page.getByText(question)).toBeVisible({ timeout: TIMEOUT });
  await expect(page.getByRole("button", { name: "Regenerate" })).toHaveCount(2, {
    timeout: TIMEOUT,
  });
});
