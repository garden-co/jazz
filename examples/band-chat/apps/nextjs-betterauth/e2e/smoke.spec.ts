import { expect, test } from "@playwright/test";

const TIMEOUT = 30_000;

test("a new member signs up, opens a room and their message survives a reload", async ({
  page,
}) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill("Ada");
  await page.getByLabel("Email").fill(`ada-${run}@band-chat.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  await expect(page.getByRole("heading", { name: "Set up your profile" })).toBeVisible({
    timeout: TIMEOUT,
  });
  await page.getByLabel("Display name").fill("Ada");
  await page.getByRole("button", { name: "Continue", exact: true }).click();

  await page.getByRole("button", { name: "Create a room" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Room name").fill("Rehearsal");
  await dialog.getByRole("button", { name: "Create room", exact: true }).click();

  const message = `Soundcheck at six (${run})`;
  await page.getByRole("textbox", { name: "Message input" }).fill(message);
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(page.getByText(message)).toBeVisible({ timeout: TIMEOUT });

  await page.reload();
  await expect(page.getByText(message)).toBeVisible({ timeout: TIMEOUT });
});
