import { expect, test } from "@playwright/test";

const TIMEOUT = 60_000;

test("a new designer gets a poster, adds a shape and it survives a reload", async ({ page }) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill("Ada");
  await page.getByLabel("Email").fill(`ada-${run}@poster-shop.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  const canvas = page.getByRole("application", { name: "Poster canvas" });
  await expect(canvas).toBeVisible({ timeout: TIMEOUT });
  const before = Number(await canvas.getAttribute("data-shape-count"));
  await page.getByRole("button", { name: "Add rectangle" }).click();
  await expect(canvas).toHaveAttribute("data-shape-count", String(before + 1), {
    timeout: TIMEOUT,
  });

  await page.reload();
  await expect(page.getByRole("application", { name: "Poster canvas" })).toHaveAttribute(
    "data-shape-count",
    String(before + 1),
    { timeout: TIMEOUT },
  );
});
