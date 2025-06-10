import { expect, test } from "@playwright/test";

test("should add and delete account in dropdown", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Account ID").fill("test-account-id");
  await page.getByLabel("Account secret").fill("test-account-secret");
  await page.getByRole("button", { name: "Add account" }).click();

  await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();
  await page.getByLabel("Account to inspect").selectOption("test-account-id");

  await page.getByRole("button", { name: "Remove account" }).click();
  await expect(page.getByText("Jazz CoValue Inspector")).not.toBeVisible();
  await expect(page.getByText("Add an account to inspect")).toBeVisible();
  await expect(page.getByText("test-account-id")).not.toBeVisible();
});
