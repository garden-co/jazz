import { expect, test } from "@playwright/test";

const TIMEOUT = 90_000;

test("a new listener streams the demo library in and it survives a reload", async ({ page }) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByLabel("Email").fill(`ada-${run}@record-player.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  const albums = page.getByRole("listbox", { name: "Albums" }).getByRole("option");
  const addDemo = page.getByRole("button", { name: "Add demo library" });
  // The library is shared by every account, so another run may have filled it.
  await expect(addDemo.or(albums.first())).toBeVisible({ timeout: TIMEOUT });
  if (await addDemo.isVisible()) await addDemo.click();
  await expect(albums.first()).toBeVisible({ timeout: TIMEOUT });
  await expect(page.getByRole("button", { name: /^Play / }).first()).toBeVisible({
    timeout: TIMEOUT,
  });

  await page.reload();
  await expect(
    page.getByRole("listbox", { name: "Albums" }).getByRole("option").first(),
  ).toBeVisible({ timeout: TIMEOUT });
});
