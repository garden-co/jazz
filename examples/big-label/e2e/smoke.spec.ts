import { expect, test } from "@playwright/test";

const TIMEOUT = 60_000;

test("a new account gets a personal label, adds an artist and it survives a reload", async ({
  page,
}) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account instead" }).click();
  await page.getByLabel("Your name").fill("Ada");
  await page.getByLabel("Email").fill(`ada-${run}@big-label.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  // The backend bootstraps a personal label with this account as its admin.
  const nav = page.getByRole("navigation");
  await expect(nav.getByRole("link", { name: "Artists" })).toBeVisible({ timeout: TIMEOUT });
  await nav.getByRole("link", { name: "Artists" }).click();
  await page.getByRole("button", { name: "Add artist" }).first().click();
  const dialog = page.getByRole("dialog");
  const artist = `The Analytical Engines ${run}`;
  await dialog.getByLabel("Name").fill(artist);
  await dialog.getByRole("button", { name: "Add artist" }).click();
  await expect(page.getByText(artist).first()).toBeVisible({ timeout: TIMEOUT });

  await page.reload();
  await expect(page.getByText(artist).first()).toBeVisible({ timeout: TIMEOUT });
});
