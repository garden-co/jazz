import { expect, test } from "@playwright/test";

test("home page loads", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Welcome!")).toBeVisible();

  await page.getByLabel("Name").fill("Bob");
  await expect(page.getByText("Welcome, Bob!")).toBeVisible();
});
