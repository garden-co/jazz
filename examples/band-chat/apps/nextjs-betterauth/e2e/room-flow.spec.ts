import { expect, test } from "@playwright/test";

test("signs up and opens the deterministic demo room", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Display name").fill("Fixture Player");
  await page.getByLabel("Email").fill("fixture-player@example.test");
  await page.getByLabel("Password").fill("fixture-password");
  await page.getByRole("button", { name: "Create account", exact: true }).click();
  await expect(page).toHaveURL(/\/dashboard/);
  await page.getByRole("button", { name: "Open demo room" }).click();
  await expect(page.getByRole("heading", { name: "# Neon Soundcheck" })).toBeVisible();
});
