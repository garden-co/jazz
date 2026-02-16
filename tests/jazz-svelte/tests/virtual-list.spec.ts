import { expect, test } from "@playwright/test";

test("should show the list and update properties", async ({ page }) => {
  await page.goto("/virtual-list");

  await page.getByRole("combobox", { name: "List" }).selectOption("alice");

  await expect(page.getByText("Alice 0")).toBeVisible();

  await page.getByPlaceholder("Filter").fill("31");

  await expect(page.getByText("Alice 31")).toBeVisible();
  await expect(page.getByText("Alice 32")).not.toBeVisible();

  await page.getByRole("combobox", { name: "List" }).selectOption("bob");

  await expect(page.getByText("Bob 31")).toBeVisible();
  await expect(page.getByText("Alice 31")).not.toBeVisible();
});
